"""Data drift: Population Stability Index (PSI) fitur model risiko antar periode.

Perbandingan:
  1. train (1957-2014)            vs test (2020-sekarang)        -> apakah dunia sudah berubah sejak data latih
  2. arsip akhir (2019-2022/07)   vs LL2 (2022/08-sekarang)       -> pergeseran akibat pergantian sumber data
PSI < 0.10 Stabil, 0.10-0.25 Moderat, > 0.25 Signifikan.
"""
import numpy as np
import pandas as pd

from .. import db
from ..runlog import track_batch
from .features import CAT_FEATURES, NUM_FEATURES, TARGET, build_features, load_history


def psi_numeric(ref, cur, bins=10):
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        edges = np.unique(np.concatenate([edges, [ref.min() - 1, ref.max() + 1]]))
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref)
    c = np.histogram(cur, edges)[0] / len(cur)
    r, c = np.clip(r, 1e-4, None), np.clip(c, 1e-4, None)
    return float(np.sum((c - r) * np.log(c / r)))


def psi_categorical(ref, cur):
    cats = set(ref) | set(cur)
    r = ref.value_counts(normalize=True).reindex(cats, fill_value=0).clip(lower=1e-4)
    c = cur.value_counts(normalize=True).reindex(cats, fill_value=0).clip(lower=1e-4)
    return float(np.sum((c - r) * np.log(c / r)))


def status(psi):
    return "Stabil" if psi < 0.10 else ("Moderat" if psi < 0.25 else "Signifikan")


def run(version, pipeline="orbit_ml_training"):
    with track_batch(pipeline, "data_drift_psi", "ml") as b:
        df = build_features(load_history())
        pairs = [("1957-2014", df[df.ts < "2015-01-01"], "2020-now", df[df.ts >= "2020-01-01"]),
                 ("arsip 2019-2022/07", df[(df.ts >= "2019-01-01") & (df.sumber_data == "Arsip Internal")],
                  "LL2 2022/08-now", df[df.sumber_data == "Launch Library 2"])]
        rows = []
        for rn, ref, cn, cur in pairs:
            if cur.empty or ref.empty:
                continue
            for f in NUM_FEATURES + [TARGET]:
                p = psi_numeric(ref[f].astype(float).values, cur[f].astype(float).values)
                rows.append((version, f, rn, cn, round(p, 5), float(ref[f].mean()), float(cur[f].mean()), status(p)))
            for f in CAT_FEATURES:
                p = psi_categorical(ref[f], cur[f])
                rows.append((version, f, rn, cn, round(p, 5), None, None, status(p)))
        with db.transaction() as conn, conn.cursor() as cur:
            db.psycopg2.extras.execute_values(cur, """INSERT INTO ml.drift_report (version, feature, ref_period,
                cur_period, psi, ref_mean, cur_mean, status) VALUES %s""", rows)
        b.rows_written = len(rows)
        sig = [r[1] for r in rows if r[7] == "Signifikan"]
        b.note(f"{len(rows)} PSI dihitung; signifikan: {sorted(set(sig))}")
        return rows
