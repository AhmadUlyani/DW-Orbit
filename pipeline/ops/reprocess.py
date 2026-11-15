"""Reprocessing dari Data Lake: membangun ulang staging + DWH tanpa memanggil API lagi.

Dipakai kalau aturan transformasi berubah atau DWH rusak sementara Data Lake masih ada. Raw zone
menyimpan data asli termasuk setiap snapshot jadwal, jadi hasilnya sama dengan pemrosesan awal.
"""
from .. import config, db
from ..runlog import track_batch
from ..transform import ll2 as tll2

RESET_SQL = """
TRUNCATE staging.stg_maven_raw, staging.stg_ll2_launch_raw, staging.stg_ll2_config_raw,
         staging.stg_peluncuran_clean, staging.stg_jadwal_clean, staging.stg_roket_clean;
TRUNCATE dw.fact_peluncuran, dw.fact_jadwal_snapshot, dw.fact_konfigurasi_roket RESTART IDENTITY;
TRUNCATE dw.dim_perusahaan, dw.dim_roket, dw.dim_lokasi RESTART IDENTITY CASCADE;
DELETE FROM dw.dim_orbit WHERE orbit_key <> -1;
ALTER SEQUENCE dw.seq_orbit_key RESTART WITH 1;
TRUNCATE ml.pred_launch_risk;
DELETE FROM meta.watermark WHERE name LIKE 'transform_%%' OR name LIKE 'load_%%';
DELETE FROM meta.lineage WHERE target_table = 'staging.stg_peluncuran_clean';
"""


def reset_for_reprocess(pipeline="orbit_reprocess"):
    with track_batch(pipeline, "reset_staging_dw", "lake") as b:
        n_obj = db.scalar("SELECT COUNT(*) FROM meta.lake_object WHERE zone='raw'")
        if not n_obj:
            raise RuntimeError("Data Lake raw kosong: reprocessing tidak mungkin")
        db.execute(RESET_SQL)
        b.note(f"staging & DWH dikosongkan; akan dibangun ulang dari {n_obj} objek raw")
        return n_obj


def transform_all_snapshots(pipeline="orbit_reprocess"):
    """Transform semua snapshot jadwal berurutan agar geser_jam (TE-05) dihitung ulang."""
    dates = [r[0] for r in db.execute("""SELECT DISTINCT snapshot_date FROM staging.stg_ll2_launch_raw
                                          WHERE endpoint='upcoming' ORDER BY 1""", fetch=True)]
    for d in dates:
        tll2.transform_snapshot(d, pipeline=pipeline)
    return len(dates)
