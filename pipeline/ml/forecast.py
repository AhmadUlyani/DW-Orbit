"""Forecasting jumlah peluncuran orbital global per bulan (regression / time series).

Kandidat (Baseline -> Improvement -> Comparison):
  naive_last       : y_hat(t) = y(t-1)
  seasonal_naive   : y_hat(t) = y(t-12)
  moving_avg_12    : y_hat(t) = rata-rata 12 bulan terakhir
  ridge_lags       : Ridge regression pada lag 1,2,3,6,12 + rolling mean 3/12 + musim (sin/cos) + tren
  hist_gb_lags     : HistGradientBoostingRegressor pada fitur yang sama
Evaluasi: backtest one-step-ahead (expanding window, model dilatih ulang tiap bulan) pada 24 bulan
terakhir yang lengkap -> MAE, RMSE, MAPE, R2. Model terpilih = MAE terkecil pada 12 bulan validasi
(sebelum periode test). Forecast 12 bulan ke depan dibuat rekursif + interval prediksi 95%
(+-1.96 x std residual backtest).
"""
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from .. import db
from ..runlog import log, track_batch

MODEL_NAME = "launch_forecast"
LAGS = (1, 2, 3, 6, 12)
TEST_MONTHS, VALID_MONTHS = 24, 12


def monthly_series():
    df = db.read_sql("""SELECT date_trunc('month', tanggal)::date AS bulan, COUNT(*) AS n
                        FROM mart.v_peluncuran GROUP BY 1 ORDER BY 1""")
    s = df.set_index(pd.to_datetime(df.bulan))["n"].astype(float)
    idx = pd.date_range(s.index.min(), pd.Timestamp.today().to_period("M").to_timestamp() - pd.offsets.MonthBegin(1),
                        freq="MS")
    return s.reindex(idx, fill_value=0.0)          # bulan berjalan (belum lengkap) tidak dipakai


def _feat(s: pd.Series, t):
    """Fitur untuk memprediksi bulan t memakai data s sampai t-1."""
    hist = s[s.index < t]
    row = {f"lag_{l}": hist.iloc[-l] if len(hist) >= l else hist.iloc[0] for l in LAGS}
    row["roll3"] = hist.iloc[-3:].mean()
    row["roll12"] = hist.iloc[-12:].mean()
    row["sin_m"], row["cos_m"] = np.sin(2 * np.pi * t.month / 12), np.cos(2 * np.pi * t.month / 12)
    row["trend"] = (t.year - 1957) + t.month / 12
    return row


def _design(s, start):
    rows, ys, idx = [], [], []
    for t in s.index[s.index >= start]:
        rows.append(_feat(s, t)); ys.append(s[t]); idx.append(t)
    return pd.DataFrame(rows, index=idx), np.array(ys)


MODELS = {
    "ridge_lags": lambda: Ridge(alpha=1.0),
    "hist_gb_lags": lambda: HistGradientBoostingRegressor(max_depth=3, learning_rate=0.05, max_iter=300,
                                                          random_state=42),
}


def _predict_one(name, s, t, train_from):
    hist = s[s.index < t]
    if name == "naive_last":
        return hist.iloc[-1]
    if name == "seasonal_naive":
        return hist.iloc[-12]
    if name == "moving_avg_12":
        return hist.iloc[-12:].mean()
    X, y = _design(hist, train_from)
    m = MODELS[name]().fit(X, y)
    return float(m.predict(pd.DataFrame([_feat(s, t)]))[0])


def _mets(y, p):
    y, p = np.asarray(y), np.asarray(p)
    nz = y > 0
    return {"mae": mean_absolute_error(y, p), "rmse": float(np.sqrt(mean_squared_error(y, p))),
            "mape": float(np.mean(np.abs((y[nz] - p[nz]) / y[nz])) * 100) if nz.any() else float("nan"),
            "r2": r2_score(y, p)}


def train(pipeline="orbit_ml_training"):
    with track_batch(pipeline, "train_launch_forecast", "mart.v_peluncuran") as b:
        version = datetime.now().strftime("%Y%m%d_%H%M%S")
        s = monthly_series()
        train_from = pd.Timestamp("1960-01-01")   # data 1957-1959 hanya untuk lag awal
        test_idx = s.index[-TEST_MONTHS:]
        valid_idx = s.index[-(TEST_MONTHS + VALID_MONTHS):-TEST_MONTHS]
        names = ["naive_last", "seasonal_naive", "moving_avg_12", *MODELS]
        preds = {n: {} for n in names}
        for n in names:
            for t in list(valid_idx) + list(test_idx):
                preds[n][t] = max(_predict_one(n, s, t, train_from), 0.0)
        res, exp_rows = {}, []
        for n in names:
            pv = [preds[n][t] for t in valid_idx]
            pt = [preds[n][t] for t in test_idx]
            res[n] = {"valid": _mets(s[valid_idx], pv), "test": _mets(s[test_idx], pt)}
            for split in ("valid", "test"):
                exp_rows += [(MODEL_NAME, version, n, split, k, float(v)) for k, v in res[n][split].items()
                             if np.isfinite(v)]
            log.info("  %-15s valid MAE=%.2f | test MAE=%.2f RMSE=%.2f MAPE=%.1f%% R2=%.3f", n,
                     res[n]["valid"]["mae"], res[n]["test"]["mae"], res[n]["test"]["rmse"],
                     res[n]["test"]["mape"], res[n]["test"]["r2"])
        champion = min(res, key=lambda k: res[k]["valid"]["mae"])
        resid = np.array([s[t] - preds[champion][t] for t in test_idx])
        sigma = float(np.std(resid, ddof=1))
        log.info("CHAMPION forecast = %s (sigma residual %.2f)", champion, sigma)

        # forecast rekursif 12 bulan ke depan dengan champion
        ext = s.copy()
        future = pd.date_range(s.index[-1] + pd.offsets.MonthBegin(1), periods=12, freq="MS")
        for t in future:
            ext.loc[t] = max(_predict_one(champion, ext, t, train_from), 0.0)
        rows = []
        for n in names:
            for t in list(valid_idx) + list(test_idx):
                rows.append((MODEL_NAME, version, n, t.date(), "Global", float(s[t]), float(preds[n][t]),
                             None, None, False))
        for h, t in enumerate(future, 1):
            band = 1.96 * sigma * np.sqrt(h) if champion in ("naive_last", "seasonal_naive") else 1.96 * sigma
            rows.append((MODEL_NAME, version, champion, t.date(), "Global", None, float(ext[t]),
                         max(float(ext[t] - band), 0.0), float(ext[t] + band), True))
        reg = [dict(model_name=MODEL_NAME, version=version, candidate=n, algorithm=n,
                    params=db.json.dumps({"lags": LAGS} if n in MODELS else {}),
                    features=[f"lag_{l}" for l in LAGS] + ["roll3", "roll12", "sin_m", "cos_m", "trend"] if n in MODELS else [],
                    train_period="1960-", valid_period=f"{valid_idx[0]:%Y-%m}..{valid_idx[-1]:%Y-%m}",
                    test_period=f"{test_idx[0]:%Y-%m}..{test_idx[-1]:%Y-%m}", n_train=len(s) - TEST_MONTHS,
                    n_test=TEST_MONTHS, metrics=db.jsonsafe({k: round(v, 4) for k, v in res[n]["test"].items()}),
                    artifact_key=None, is_champion=n == champion) for n in names]
        with db.transaction() as conn, conn.cursor() as cur:
            cur.execute("UPDATE ml.model_registry SET is_champion=FALSE WHERE model_name=%s", (MODEL_NAME,))
            db.insert_df(cur, "ml.model_registry", pd.DataFrame(reg))
            db.psycopg2.extras.execute_values(cur, """INSERT INTO ml.model_experiment (model_name, version, candidate,
                split, metric, value) VALUES %s""", exp_rows)
            db.psycopg2.extras.execute_values(cur, """INSERT INTO ml.forecast_monthly (model_name, version, candidate,
                bulan, segment, actual, predicted, lower_pi, upper_pi, is_future) VALUES %s""", rows)
        b.rows_read, b.rows_written = len(s), len(rows)
        b.note(f"versi {version}, champion {champion}: test MAE {res[champion]['test']['mae']:.2f}, "
               f"MAPE {res[champion]['test']['mape']:.1f}%")
        return version
