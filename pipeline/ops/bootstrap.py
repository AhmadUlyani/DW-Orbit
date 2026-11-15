"""Bootstrap: DDL semua layer, seed mapping, dan katalog metadata. Aman dijalankan ulang."""
import os

import pandas as pd

from .. import config, db
from ..runlog import track_batch
from ..transform.common import seed_mappings


def apply_ddl(pipeline="orbit_init"):
    with track_batch(pipeline, "apply_ddl", "sql") as b:
        files = sorted(config.DDL_DIR.glob("*.sql"))
        subs = {":bi_user": os.getenv("DW_BI_USER", "bi_reader"),
                ":analyst_user": os.getenv("DW_ANALYST_USER", "analyst_ro")}
        with db.transaction() as conn, conn.cursor() as cur:
            for f in files:
                sql = f.read_text(encoding="utf-8")
                for k, v in subs.items():
                    sql = sql.replace(k, v)
                cur.execute(sql)
        b.note(f"{len(files)} file DDL diterapkan: {[f.name for f in files]}")
        return len(files)


def seed_catalog(pipeline="orbit_init"):
    """Isi meta.data_catalog dari docs/data_catalog.csv dan muat ulang mapping."""
    with track_batch(pipeline, "seed_catalog_mappings", "repo") as b:
        counts = seed_mappings()
        df = pd.read_csv(config.PROJECT_DIR / "docs" / "data_catalog.csv", dtype=str, keep_default_na=False)
        df = df.replace({"": None})
        with db.transaction() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE meta.data_catalog")
            n = db.insert_df(cur, "meta.data_catalog", df)
        b.rows_written = n + sum(counts.values())
        b.note(f"katalog {n} tabel, mapping {counts}")
        return n
