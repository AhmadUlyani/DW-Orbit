"""Export ke zona Silver (orbit-clean) dan Gold (orbit-curated) Data Lake dalam format Parquet.

Silver : staging clean per dataset, dipartisi per tanggal proses
Gold   : snapshot view mart.*
"""
from .. import db, lake
from ..ingest.ll2 import today_wita
from ..runlog import track_batch, write_lineage

SILVER = {
    "peluncuran_clean": "SELECT * FROM staging.stg_peluncuran_clean",
    "jadwal_clean": "SELECT * FROM staging.stg_jadwal_clean WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM staging.stg_jadwal_clean)",
    "roket_clean": "SELECT * FROM staging.stg_roket_clean WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM staging.stg_roket_clean)",
}
GOLD = ["v_peluncuran", "v_keandalan_tahunan", "v_biaya_roket", "v_jadwal_terkini", "v_pergeseran_jadwal"]


def export_lake(pipeline="orbit_daily_etl"):
    d = today_wita()
    with track_batch(pipeline, "export_lake_silver_gold", "dw") as b:
        keys = []
        for name, sql in SILVER.items():
            df = db.read_sql(sql)
            k, _ = lake.put_parquet("clean", f"dataset={name}/process_date={d:%Y-%m-%d}/part-0000.parquet", df,
                                    source="pipeline", dataset=name, batch_id=b.batch_id, ingest_date=d)
            keys.append(k); b.rows_written += len(df)
        for v in GOLD:
            df = db.read_sql(f"SELECT * FROM mart.{v}")
            k, _ = lake.put_parquet("curated", f"mart={v}/process_date={d:%Y-%m-%d}/part-0000.parquet", df,
                                    source="pipeline", dataset=v, batch_id=b.batch_id, ingest_date=d)
            keys.append(k); b.rows_written += len(df)
        write_lineage(b.batch_id, "lake:orbit-clean,orbit-curated", "lake", keys, ["EXPORT-PARQUET"], b.rows_written)
        b.note(f"{len(keys)} objek parquet")
        return keys
