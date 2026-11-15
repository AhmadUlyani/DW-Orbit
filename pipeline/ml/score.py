"""Scoring harian: probabilitas kegagalan untuk setiap jadwal pada snapshot terbaru (model champion).

Output ml.pred_launch_risk dipakai Data Mart Jadwal (mart.v_jadwal_terkini) & dashboard:
  risk_band  Rendah / Sedang / Tinggi berdasarkan persentil skor model pada test set
  faktor_utama  penjelasan singkat berbasis fitur (mis. 'roket baru (0 penerbangan)')
"""
import io

import joblib
import pandas as pd

from .. import db, lake
from ..runlog import track_batch
from .features import FEATURES, features_for_upcoming, load_history


def load_champion(model_name="launch_risk"):
    row = db.execute("""SELECT artifact_key, version FROM ml.model_registry
                        WHERE model_name=%s AND is_champion AND artifact_key IS NOT NULL
                        ORDER BY trained_at DESC LIMIT 1""", (model_name,), fetch=True)
    if not row:
        return None
    return joblib.load(io.BytesIO(lake.get_bytes(row[0][0])))


def _factors(r):
    f = []
    if r.roket_n_prior == 0:
        f.append("penerbangan perdana roket")
    elif r.roket_n_prior < 3:
        f.append(f"roket baru ({int(r.roket_n_prior)} penerbangan)")
    if r.roket_fail_rate_prior >= 0.15:
        f.append(f"riwayat gagal roket {r.roket_fail_rate_prior:.0%}")
    if r.prov_fail_last10 >= 2:
        f.append(f"{int(r.prov_fail_last10)} gagal dari 10 terakhir penyedia")
    if r.prov_n_prior < 5:
        f.append("penyedia minim pengalaman")
    return "; ".join(f) or "rekam jejak roket & penyedia baik"


def score_upcoming(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "ml_score_upcoming", "ml") as b:
        art = load_champion()
        if art is None:
            b.status = "SKIPPED"; b.note("belum ada model champion (jalankan orbit_ml_training)"); return 0
        up = db.read_sql("""SELECT j.ll2_launch_id::text AS ll2_launch_id, t.tanggal AS snapshot_date, j.net_waktu,
                                   p.nama_perusahaan, p.jenis_perusahaan, r.nama_roket, l.kode_negara
                            FROM dw.fact_jadwal_snapshot j
                            JOIN dw.dim_tanggal t ON t.date_key = j.snapshot_date_key
                            JOIN dw.dim_perusahaan p ON p.perusahaan_key = j.perusahaan_key
                            JOIN dw.dim_roket r ON r.roket_key = j.roket_key
                            JOIN dw.dim_lokasi l ON l.lokasi_key = j.lokasi_key
                            WHERE j.snapshot_date_key = (SELECT MAX(snapshot_date_key) FROM dw.fact_jadwal_snapshot)""")
        if up.empty:
            b.status = "SKIPPED"; b.note("tidak ada jadwal"); return 0
        X = features_for_upcoming(load_history(), up)
        p = art["model"].predict_proba(X[FEATURES])[:, 1]
        # ambang = persentil ke-70 & ke-90 prediksi model pada test set 2020-sekarang:
        # Tinggi = termasuk 10% peluncuran paling berisiko, Sedang = 20% berikutnya
        q70, q90 = art.get("band_q70", 0.1), art.get("band_q90", 0.2)
        X["p_gagal"] = p
        X["risk_band"] = pd.cut(p, [-1, q70, q90, 2], labels=["Rendah", "Sedang", "Tinggi"]).astype(str)
        X["faktor_utama"] = X.apply(_factors, axis=1)
        rows = [(r.ll2_launch_id, r.snapshot_date, art["version"], float(r.p_gagal), r.risk_band, r.faktor_utama)
                for r in X.itertuples()]
        with db.transaction() as conn, conn.cursor() as cur:
            db.psycopg2.extras.execute_values(cur, """INSERT INTO ml.pred_launch_risk (ll2_launch_id, snapshot_date,
                model_version, p_gagal, risk_band, faktor_utama) VALUES %s
                ON CONFLICT (ll2_launch_id, snapshot_date) DO UPDATE SET model_version=EXCLUDED.model_version,
                p_gagal=EXCLUDED.p_gagal, risk_band=EXCLUDED.risk_band, faktor_utama=EXCLUDED.faktor_utama,
                scored_at=now()""", rows)
        b.rows_read, b.rows_written = len(up), len(rows)
        b.note(f"{len(rows)} jadwal diskor dengan model {art['candidate']} v{art['version']}; "
               f"{(X.risk_band == 'Tinggi').sum()} berisiko tinggi")
        return len(rows)
