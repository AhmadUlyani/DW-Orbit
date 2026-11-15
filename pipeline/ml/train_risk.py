"""Model risiko kegagalan peluncuran (binary classification, target is_gagal).

Alur: Baseline -> Model/Improvement -> Comparison -> Model terpilih (champion)
  B0 baseline_prior      : probabilitas = tingkat gagal data latih (tanpa fitur) -> batas bawah
  B1 baseline_provider   : skor = tingkat gagal historis penyedia (heuristik yang biasa dipakai analis)
  M1 logreg              : Logistic Regression multi-fitur
  M2 random_forest       : Random Forest (non-linear, bagging)
  M3 hist_gb             : HistGradientBoosting (boosting)

Validasi (urutan waktu dipertahankan, tidak diacak):
  - Time-series cross-validation expanding window, 4 fold: validasi 2000-04, 2005-09, 2010-14, 2015-19
    (latih memakai seluruh data sebelum periode validasi). Hyperparameter & champion dipilih dari
    rata-rata PR-AUC empat fold, karena satu periode validasi punya terlalu sedikit kejadian gagal.
  - Test (hold-out) : 2020 - sekarang, dilatih pada 1957-2019. Mencakup data arsip & LL2.
    Test tidak dipakai untuk memilih model.
Metrik: ROC-AUC, PR-AUC (average precision; cocok untuk kelas tidak seimbang), Brier, log-loss,
precision/recall/F1 + confusion matrix pada ambang F1-optimal dari fold validasi terakhir.
"""
import copy
import io
import json
import warnings
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score, brier_score_loss, confusion_matrix, f1_score, log_loss,
                             precision_score, recall_score, roc_auc_score)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .. import db, lake
from ..runlog import log, track_batch
from .features import CAT_FEATURES, FEATURES, NUM_FEATURES, TARGET, build_features, load_history

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*iprint.*")

MODEL_NAME = "launch_risk"
CV_FOLDS = [("2000-01-01", "2005-01-01"), ("2005-01-01", "2010-01-01"),
            ("2010-01-01", "2015-01-01"), ("2015-01-01", "2020-01-01")]
TEST_START = "2020-01-01"
SEED = 42


def _prep(scale):
    num = ("num", StandardScaler() if scale else "passthrough", NUM_FEATURES)
    cat = ("cat", OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=30, sparse_output=False),
           CAT_FEATURES)
    return ColumnTransformer([num, cat])


class PriorModel:
    """B0: probabilitas konstan = tingkat gagal pada data latih."""
    def fit(self, X, y):
        self.p_ = float(np.mean(y)); return self

    def predict_proba(self, X):
        return np.column_stack([np.full(len(X), 1 - self.p_), np.full(len(X), self.p_)])


class ProviderRateModel:
    """B1: skor = tingkat gagal historis penyedia (fitur point-in-time), tanpa pembelajaran."""
    def fit(self, X, y):
        return self

    def predict_proba(self, X):
        p = X["prov_fail_rate_prior"].clip(0, 1).values
        return np.column_stack([1 - p, p])


def candidates():
    yield "baseline_prior", lambda: PriorModel(), {}
    yield "baseline_provider", lambda: ProviderRateModel(), {}
    for C in (0.1, 1.0):
        for cw in (None, "balanced"):
            yield "logreg", (lambda C=C, cw=cw: Pipeline([("prep", _prep(True)), ("clf", LogisticRegression(
                C=C, class_weight=cw, max_iter=3000, random_state=SEED))])), {"C": C, "class_weight": cw}
    for depth in (6, 10):
        for leaf in (5, 20):
            yield "random_forest", (lambda d=depth, l=leaf: Pipeline([("prep", _prep(False)), (
                "clf", RandomForestClassifier(n_estimators=300, max_depth=d, min_samples_leaf=l, n_jobs=2,
                                              random_state=SEED))])), \
                {"n_estimators": 300, "max_depth": depth, "min_samples_leaf": leaf}
    for lr in (0.03, 0.1):
        for depth in (3, 5):
            yield "hist_gb", (lambda lr=lr, d=depth: Pipeline([("prep", _prep(False)), (
                "clf", HistGradientBoostingClassifier(learning_rate=lr, max_depth=d, max_iter=200,
                                                      l2_regularization=1.0, random_state=SEED))])), \
                {"learning_rate": lr, "max_depth": depth, "max_iter": 200, "l2_regularization": 1.0}


def _metrics(y, p, thr):
    yhat = (p >= thr).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, yhat, labels=[0, 1]).ravel()
    return {"roc_auc": roc_auc_score(y, p), "pr_auc": average_precision_score(y, p),
            "brier": brier_score_loss(y, p), "log_loss": log_loss(y, np.clip(p, 1e-6, 1 - 1e-6), labels=[0, 1]),
            "precision": precision_score(y, yhat, zero_division=0), "recall": recall_score(y, yhat, zero_division=0),
            "f1": f1_score(y, yhat, zero_division=0), "threshold": thr,
            "tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn), "base_rate": float(np.mean(y)),
            "n": int(len(y))}


def _best_threshold(y, p):
    grid = np.unique(np.quantile(p, np.linspace(0.5, 0.995, 120)))
    f1s = [f1_score(y, (p >= t).astype(int), zero_division=0) for t in grid]
    return float(grid[int(np.argmax(f1s))]) if len(grid) else 0.5


def _cv(df, make):
    """Expanding-window time-series CV -> (mean PR-AUC, mean ROC-AUC, ambang dari fold terakhir, metrik per fold)."""
    folds = []
    for start, end in CV_FOLDS:
        tr, va = df[df.ts < start], df[(df.ts >= start) & (df.ts < end)]
        m = make().fit(tr[FEATURES], tr[TARGET])
        p = m.predict_proba(va[FEATURES])[:, 1]
        folds.append((start[:4], average_precision_score(va[TARGET], p), roc_auc_score(va[TARGET], p),
                      va[TARGET].values, p))
    thr = _best_threshold(folds[-1][3], folds[-1][4])
    return float(np.mean([f[1] for f in folds])), float(np.mean([f[2] for f in folds])), thr, folds


def train(pipeline="orbit_ml_training"):
    with track_batch(pipeline, "train_launch_risk", "mart.v_peluncuran") as b:
        version = datetime.now().strftime("%Y%m%d_%H%M%S")
        df = build_features(load_history())
        trva, te = df[df.ts < TEST_START], df[df.ts >= TEST_START]
        b.rows_read = len(df)
        log.info("data %s baris: train+cv=%s (gagal %.3f), test=%s (gagal %.3f)", len(df), len(trva),
                 trva[TARGET].mean(), len(te), te[TARGET].mean())

        # 1) time-series CV: pilih hyperparameter terbaik per kandidat (rata-rata PR-AUC 4 fold)
        best, exp_rows = {}, []
        for name, make, params in candidates():
            ap, auc, thr, folds = _cv(trva, make)
            log.info("  %-18s %-70s CV PR-AUC=%.4f ROC-AUC=%.4f", name, json.dumps(params), ap, auc)
            if name not in best or ap > best[name]["cv_pr_auc"]:
                best[name] = dict(make=make, params=params, cv_pr_auc=ap, cv_roc_auc=auc, thr=thr, folds=folds)
        # 2) latih ulang pada 1957-2019, evaluasi hold-out TEST 2020-sekarang
        bt_rows = []
        for name, r in best.items():
            m = r["make"]().fit(trva[FEATURES], trva[TARGET])
            pt = m.predict_proba(te[FEATURES])[:, 1]
            r.update(model=m, p_test=pt, test=_metrics(te[TARGET].values, pt, r["thr"]))
            exp_rows += [(MODEL_NAME, version, name, "cv_mean", "pr_auc", r["cv_pr_auc"]),
                         (MODEL_NAME, version, name, "cv_mean", "roc_auc", r["cv_roc_auc"])]
            exp_rows += [(MODEL_NAME, version, name, f"cv_{f[0]}", k, v) for f in r["folds"]
                         for k, v in (("pr_auc", f[1]), ("roc_auc", f[2]))]
            exp_rows += [(MODEL_NAME, version, name, "test", k, float(v)) for k, v in r["test"].items()]
            for yr, g in te.assign(p=pt).groupby(te.ts.dt.year):                   # stabilitas per tahun
                if g[TARGET].nunique() == 2:
                    exp_rows.append((MODEL_NAME, version, name, f"test_{yr}", "roc_auc",
                                     roc_auc_score(g[TARGET], g.p)))
                    exp_rows.append((MODEL_NAME, version, name, f"test_{yr}", "pr_auc",
                                     average_precision_score(g[TARGET], g.p)))
                exp_rows.append((MODEL_NAME, version, name, f"test_{yr}", "n", float(len(g))))
                exp_rows.append((MODEL_NAME, version, name, f"test_{yr}", "base_rate", float(g[TARGET].mean())))
            bt_rows += [(MODEL_NAME, version, name, sid, tgl, int(y), float(p), int(p >= r["thr"]))
                        for sid, tgl, y, p in zip(te.sumber_id, te.tanggal.dt.date, te[TARGET], pt)]
            log.info("  TEST %-18s ROC-AUC=%.4f PR-AUC=%.4f P=%.3f R=%.3f F1=%.3f Brier=%.4f", name,
                     r["test"]["roc_auc"], r["test"]["pr_auc"], r["test"]["precision"], r["test"]["recall"],
                     r["test"]["f1"], r["test"]["brier"])
        champion = max(best, key=lambda k: best[k]["cv_pr_auc"])
        ch = best[champion]
        log.info("CHAMPION = %s (CV PR-AUC %.4f)", champion, ch["cv_pr_auc"])
        # importance dihitung pada model 1957-2019 (sebelum refit) agar test tidak bocor
        imp_rows = _importance(ch["model"], te, version, champion)
        # 3) model produksi: champion dilatih ulang pada seluruh data berlabel
        prod = ch["make"]().fit(df[FEATURES], df[TARGET])
        q = np.quantile(ch["p_test"], [0.70, 0.90])          # ambang risk band = persentil prediksi test
        buf = io.BytesIO()
        joblib.dump({"model": prod, "features": FEATURES, "threshold": ch["thr"], "version": version,
                     "candidate": champion, "band_q70": float(q[0]), "band_q90": float(q[1]),
                     "base_rate_recent": float(te[TARGET].mean())}, buf)
        art_key, _ = lake.put_bytes("curated", f"models/{MODEL_NAME}/{version}/model.joblib", buf.getvalue(),
                                    source="ml", dataset=f"model_{MODEL_NAME}", batch_id=b.batch_id)
        reg_rows = [dict(model_name=MODEL_NAME, version=version, candidate=name,
                         algorithm=(type(r["model"].steps[-1][1]).__name__ if hasattr(r["model"], "steps")
                                    else type(r["model"]).__name__),
                         params=json.dumps(r["params"]), features=FEATURES,
                         train_period="1957-2019 (CV 4 fold 2000-2019)", valid_period="TS-CV expanding window",
                         test_period="2020-sekarang", n_train=len(trva), n_test=len(te),
                         metrics=db.jsonsafe({**{k: round(float(v), 6) for k, v in r["test"].items()},
                                             "cv_pr_auc": round(r["cv_pr_auc"], 6),
                                             "cv_roc_auc": round(r["cv_roc_auc"], 6)}),
                         artifact_key=art_key if name == champion else None, is_champion=name == champion)
                    for name, r in best.items()]
        with db.transaction() as conn, conn.cursor() as cur:
            cur.execute("UPDATE ml.model_registry SET is_champion=FALSE WHERE model_name=%s", (MODEL_NAME,))
            db.insert_df(cur, "ml.model_registry", pd.DataFrame(reg_rows))
            db.psycopg2.extras.execute_values(cur, """INSERT INTO ml.model_experiment (model_name, version, candidate,
                split, metric, value) VALUES %s""", exp_rows)
            db.psycopg2.extras.execute_values(cur, """INSERT INTO ml.pred_launch_backtest (model_name, version,
                candidate, sumber_id, tanggal, actual_gagal, p_gagal, pred_gagal) VALUES %s""", bt_rows)
            if imp_rows:
                db.psycopg2.extras.execute_values(cur, """INSERT INTO ml.feature_importance (model_name, version,
                    candidate, feature, importance, importance_std, method) VALUES %s""", imp_rows)
        b.rows_written = len(reg_rows)
        b.note(f"versi {version}, champion {champion}: CV PR-AUC {ch['cv_pr_auc']:.3f}; test ROC-AUC "
               f"{ch['test']['roc_auc']:.3f}, PR-AUC {ch['test']['pr_auc']:.3f} (base rate {ch['test']['base_rate']:.3f})")
        return version


def _importance(model, te, version, cand):
    """Permutation importance (penurunan PR-AUC saat fitur diacak) pada test set."""
    if not hasattr(model, "steps"):
        return []
    res = permutation_importance(model, te[FEATURES], te[TARGET], scoring="average_precision",
                                 n_repeats=10, random_state=SEED, n_jobs=1)
    return [(MODEL_NAME, version, cand, f, float(m), float(s), "permutation_pr_auc")
            for f, m, s in zip(FEATURES, res.importances_mean, res.importances_std)]
