"""Helper koneksi PostgreSQL (role etl_writer)."""
from contextlib import contextmanager
import json
import psycopg2
import psycopg2.extras
import numpy as np
import pandas as pd
from psycopg2.extensions import AsIs, register_adapter
from . import config

# adapter tipe numpy agar bisa langsung di-insert
for _t in (np.float64, np.float32):
    register_adapter(_t, lambda v: AsIs("NULL") if np.isnan(v) else AsIs(repr(float(v))))
for _t in (np.int64, np.int32, np.int16, np.int8):
    register_adapter(_t, lambda v: AsIs(int(v)))
register_adapter(np.bool_, lambda v: AsIs("TRUE" if v else "FALSE"))


def connect(**overrides):
    params = {**config.DW, **overrides}
    conn = psycopg2.connect(connect_timeout=10, application_name="orbit-pipeline", **params)
    return conn


@contextmanager
def transaction(**overrides):
    """Satu transaksi: commit kalau sukses, rollback kalau ada exception."""
    conn = connect(**overrides)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(sql, params=None, fetch=False):
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        if fetch:
            return cur.fetchall()
        return cur.rowcount


def scalar(sql, params=None):
    rows = execute(sql, params, fetch=True)
    return rows[0][0] if rows else None


def read_sql(sql, params=None) -> pd.DataFrame:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


def jsonsafe(d: dict) -> str:
    """json.dumps yang mengganti NaN/inf dengan null (JSONB menolak NaN)."""
    def clean(x):
        if isinstance(x, dict):
            return {k: clean(v) for k, v in x.items()}
        if isinstance(x, (float, np.floating)) and not np.isfinite(x):
            return None
        return x.item() if hasattr(x, "item") else x
    return json.dumps(clean(d), default=str)


def _py(v):
    """Ubah nilai pandas/numpy ke tipe Python biasa untuk psycopg2."""
    if v is None:
        return None
    if isinstance(v, (list, tuple, dict)):
        return _py_container(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(v, "item"):
        return v.item()
    if isinstance(v, dict):
        return json.dumps(v)          # kolom jsonb
    if isinstance(v, (list, tuple)):
        return list(v)                # kolom ARRAY (mis. TEXT[])
    return v


def _py_container(v):
    return json.dumps(v) if isinstance(v, dict) else list(v)


def insert_df(cur, table, df: pd.DataFrame, page_size=1000, on_conflict=""):
    if df.empty:
        return 0
    cols = list(df.columns)
    rows = [tuple(_py(v) for v in r) for r in df.itertuples(index=False, name=None)]
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES %s {on_conflict}"
    psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)
