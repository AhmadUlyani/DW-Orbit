"""Ingestion arsip internal space_missions.csv (Maven Analytics).

data/source/ -> Data Lake raw zone (file asli) -> staging.stg_maven_raw
Checksum SHA-256 disimpan; file yang tidak berubah dilewati, file yang dikoreksi diingest ulang.
Header harus sama dengan 9 kolom di kamus data.
"""
import io
from datetime import date

import pandas as pd

from .. import config, db, lake
from ..runlog import track_batch, write_lineage

CSV_NAME = "space_missions.csv"
DICT_NAME = "space_missions_data_dictionary.csv"
EXPECTED_COLUMNS = ["Company", "Location", "Date", "Time", "Rocket", "Mission",
                    "RocketStatus", "Price", "MissionStatus"]
ENCODING = "ISO-8859-1"


class SchemaError(RuntimeError):
    pass


def ingest_maven(run_date: date | None = None, pipeline="orbit_init"):
    run_date = run_date or date.today()
    with track_batch(pipeline, "ingest_maven_csv", "maven_csv") as b:
        data = (config.SOURCE_DIR / CSV_NAME).read_bytes()
        ddict = (config.SOURCE_DIR / DICT_NAME).read_bytes()
        digest = lake.sha256(data)
        existing = db.scalar("SELECT object_key FROM meta.lake_object WHERE dataset='space_missions' "
                             "AND sha256=%s ORDER BY created_at LIMIT 1", (digest,))
        if existing:
            b.status = "SKIPPED"
            b.note(f"checksum sama dengan objek {existing}, ingestion dilewati")
            return existing

        # validasi struktur terhadap kamus data
        header = pd.read_csv(io.BytesIO(data), encoding=ENCODING, nrows=0).columns.tolist()
        dict_fields = pd.read_csv(io.BytesIO(ddict))["Field"].tolist()
        if header != EXPECTED_COLUMNS or dict_fields != EXPECTED_COLUMNS:
            raise SchemaError(f"kolom CSV {header} tidak sesuai kamus data {dict_fields}")
        n_rows = data.count(b"\n") - 1
        b.rows_read = n_rows

        key, _ = lake.put_bytes("raw", lake.raw_key("maven", "space_missions", run_date, b.batch_id, CSV_NAME),
                                data, "text/csv", source="maven", dataset="space_missions",
                                batch_id=b.batch_id, ingest_date=run_date, record_count=n_rows)
        lake.put_bytes("raw", lake.raw_key("maven", "space_missions_dictionary", run_date, b.batch_id, DICT_NAME),
                       ddict, "text/csv", source="maven", dataset="space_missions_dictionary",
                       batch_id=b.batch_id, ingest_date=run_date, record_count=len(dict_fields))
        b.rows_written = n_rows
        b.note(f"{n_rows} baris -> {key} (sha256 {digest[:12]}...)")
        return key


def stage_maven(pipeline="orbit_init"):
    """Raw zone -> staging.stg_maven_raw (semua kolom TEXT, tanpa transformasi)."""
    with track_batch(pipeline, "stage_maven_raw", "maven_csv") as b:
        obj = db.execute("""SELECT object_key, sha256 FROM meta.lake_object WHERE dataset='space_missions'
                            ORDER BY created_at DESC LIMIT 1""", fetch=True)
        if not obj:
            raise RuntimeError("belum ada objek space_missions di Data Lake")
        key, digest = obj[0]
        already = db.scalar("SELECT COUNT(*) FROM staging.stg_maven_raw WHERE source_object=%s", (key,))
        if already:
            b.status = "SKIPPED"
            b.note(f"{key} sudah ada di staging ({already} baris)")
            return 0
        raw = lake.get_bytes(key)
        if lake.sha256(raw) != digest:
            raise RuntimeError(f"checksum objek {key} tidak cocok: data corrupt")
        df = pd.read_csv(io.BytesIO(raw), encoding=ENCODING, dtype=str, keep_default_na=False)
        df = df.replace({"": None})
        df.columns = ["company", "location", "date", "time", "rocket", "mission",
                      "rocket_status", "price", "mission_status"]
        df.insert(0, "row_num", range(1, len(df) + 1))
        df.insert(0, "batch_id", b.batch_id)
        df["source_object"], df["source_sha256"] = key, digest
        b.rows_read = len(df)
        with db.transaction() as conn, conn.cursor() as cur:
            b.rows_written = db.insert_df(cur, "staging.stg_maven_raw", df)
        write_lineage(b.batch_id, "staging.stg_maven_raw", "staging", [key], ["EXTRACT-AS-IS"], b.rows_written)
        return b.rows_written
