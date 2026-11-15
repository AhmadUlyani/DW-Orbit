"""Uji skalabilitas & performa pipeline/query.

Data uji sintetis dan berlabel (sumber_data='SINTETIS-PERF', sumber_id 'SYN-...'), disimpan di schema
`perf`. Data ini tidak masuk dw/mart dan tidak dipakai untuk analitik atau ML.

Pengujian (hasil -> meta.perf_result):
  T1 transform_throughput : aturan transform arsip (TC-02/03, TS-06/07, TE-01) pada N baris mentah
  T2 lake_write_parquet    : tulis N baris ke Data Lake (Parquet/snappy) -> MB/s & rows/s
  T3 staging_bulk_insert   : bulk insert N baris ke PostgreSQL (execute_values)
  T4 fact_load_sql         : load fakta dengan lookup surrogate key (INSERT..SELECT JOIN 5 dimensi)
  T5 query_latency         : query OLAP (tren tahunan per penyedia) dengan vs tanpa index
  T6 snapshot_query        : query "snapshot jadwal terbaru" pada 1-3 tahun snapshot harian, dengan vs tanpa index
"""
import hashlib
import io
import time

import numpy as np
import pandas as pd
import psutil

from .. import db, lake
from ..runlog import log, track_batch
from ..transform.common import Maps
from ..transform.maven import fix_text, parse_lokasi

SCALES = (1, 5, 10, 25, 50)
LABEL = "SINTETIS-PERF"


def _mem():
    return psutil.Process().memory_info().rss / 1024 / 1024


def _rec(run_label, test, stage, scale, rows, dur, notes=""):
    db.execute("""INSERT INTO meta.perf_result (run_label, test_name, stage, scale_factor, rows, duration_s,
                      throughput_rps, peak_mem_mb, notes) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
               (run_label, test, stage, scale, rows, round(dur, 4), round(rows / dur, 2) if dur else None,
                round(_mem(), 1), notes))
    log.info("PERF %-22s %-10s x%-4s rows=%-8s %.3fs  %.0f rows/s", test, stage, scale, rows, dur,
             rows / dur if dur else 0)


def _synthetic(base: pd.DataFrame, scale):
    out = pd.concat([base] * scale, ignore_index=True)
    out["sumber_id"] = [f"SYN-{i:09d}" for i in range(len(out))]
    out["sumber_data"] = LABEL
    return out


def run(run_label=None, pipeline="orbit_perf_test", scales=SCALES):
    run_label = run_label or time.strftime("perf_%Y%m%d_%H%M%S")
    with track_batch(pipeline, "scalability_test", "perf") as b:
        raw = db.read_sql("SELECT company, location, date, time, rocket, mission FROM staging.stg_maven_raw "
                          "WHERE batch_id = (SELECT MAX(batch_id) FROM staging.stg_maven_raw)")
        clean = db.read_sql("SELECT * FROM staging.stg_peluncuran_clean")
        maps = Maps()
        db.execute("""DROP TABLE IF EXISTS perf.stg_peluncuran_syn, perf.fact_peluncuran_syn, perf.jadwal_syn;
                      CREATE TABLE perf.stg_peluncuran_syn (LIKE staging.stg_peluncuran_clean);
                      CREATE TABLE perf.fact_peluncuran_syn (LIKE dw.fact_peluncuran INCLUDING DEFAULTS);""")
        for s in scales:
            # T1 transform CPU
            r = pd.concat([raw] * s, ignore_index=True)
            t0 = time.perf_counter()
            for row in r.itertuples(index=False):
                fix_text(row.mission, "mission", maps.koreksi); fix_text(row.location, "location", maps.koreksi)
                parse_lokasi(row.location, maps.negara)
                hashlib.md5(f"{row.company}|{row.date}|{row.time}|{row.rocket}|{row.mission}".encode()).hexdigest()
            _rec(run_label, "T1_transform_throughput", "transform", s, len(r), time.perf_counter() - t0)
            syn = _synthetic(clean, s)
            # T2 lake write
            t0 = time.perf_counter()
            buf = io.BytesIO(); syn.to_parquet(buf, index=False, compression="snappy")
            lake.put_bytes("curated", f"perf/{run_label}/x{s}.parquet", buf.getvalue(), register=False)
            _rec(run_label, "T2_lake_write_parquet", "ingest", s, len(syn), time.perf_counter() - t0,
                 f"{len(buf.getvalue()) / 1e6:.2f} MB")
            # T3 bulk insert staging
            db.execute("TRUNCATE perf.stg_peluncuran_syn")
            t0 = time.perf_counter()
            with db.transaction() as conn, conn.cursor() as cur:
                db.insert_df(cur, "perf.stg_peluncuran_syn", syn, page_size=5000)
            _rec(run_label, "T3_staging_bulk_insert", "staging", s, len(syn), time.perf_counter() - t0)
            # T4 fact load dengan lookup 5 dimensi
            db.execute("TRUNCATE perf.fact_peluncuran_syn")
            t0 = time.perf_counter()
            n = db.execute("""INSERT INTO perf.fact_peluncuran_syn (sumber_id, date_key, perusahaan_key, roket_key,
                    lokasi_key, status_key, orbit_key, waktu_peluncuran, waktu_diketahui, nama_misi, harga_musd,
                    is_sukses, sumber_data)
                SELECT c.sumber_id, TO_CHAR(c.tanggal,'YYYYMMDD')::int, p.perusahaan_key, r.roket_key, l.lokasi_key,
                       s.status_key, COALESCE(o.orbit_key,-1), c.waktu, c.waktu_diketahui, c.nama_misi, c.harga_musd,
                       c.is_sukses, c.sumber_data
                FROM perf.stg_peluncuran_syn c
                JOIN dw.dim_perusahaan p ON p.nama_perusahaan = c.perusahaan_nama
                JOIN dw.dim_roket r ON r.nama_roket = c.roket_nama AND c.tanggal BETWEEN r.start_date AND r.end_date
                JOIN dw.dim_lokasi l ON COALESCE(l.landasan,'') = COALESCE(c.landasan,'') AND l.situs = c.situs
                                    AND COALESCE(l.kode_negara,'---') = COALESCE(c.kode_negara,'---')
                JOIN dw.dim_status s ON s.nama_status = c.status_nama
                LEFT JOIN dw.dim_orbit o ON o.singkatan = c.orbit_singkatan""")
            _rec(run_label, "T4_fact_load_sql", "load", s, n, time.perf_counter() - t0)
            # T5 query OLAP tanpa vs dengan index
            q = """SELECT t.tahun, p.nama_perusahaan, COUNT(*), AVG(f.is_sukses)
                   FROM perf.fact_peluncuran_syn f JOIN dw.dim_tanggal t ON t.date_key = f.date_key
                   JOIN dw.dim_perusahaan p ON p.perusahaan_key = f.perusahaan_key
                   WHERE t.tahun BETWEEN 2015 AND 2022 AND p.nama_perusahaan IN ('SpaceX','CASC')
                   GROUP BY 1, 2"""
            db.execute("DROP INDEX IF EXISTS perf.ix_syn_date; DROP INDEX IF EXISTS perf.ix_syn_prov; ANALYZE perf.fact_peluncuran_syn")
            _rec(run_label, "T5_query_olap", "no_index", s, n, _time_query(q))
            db.execute("CREATE INDEX ix_syn_date ON perf.fact_peluncuran_syn (date_key);"
                       "CREATE INDEX ix_syn_prov ON perf.fact_peluncuran_syn (perusahaan_key, date_key);"
                       "ANALYZE perf.fact_peluncuran_syn")
            _rec(run_label, "T5_query_olap", "with_index", s, n, _time_query(q))
            b.rows_written += n
        _snapshot_test(run_label)
        db.execute("DROP TABLE IF EXISTS perf.stg_peluncuran_syn, perf.fact_peluncuran_syn, perf.jadwal_syn")
        b.note(f"run {run_label}: skala {list(scales)} (data sintetis berlabel {LABEL}, schema perf)")
        return run_label


def _time_query(q, repeat=5):
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(q); cur.fetchall()                       # warm-up
            ts = []
            for _ in range(repeat):
                t0 = time.perf_counter(); cur.execute(q); cur.fetchall(); ts.append(time.perf_counter() - t0)
        return float(np.median(ts))
    finally:
        conn.close()


def _snapshot_test(run_label):
    """T6: fact_jadwal_snapshot sintetis = snapshot harian nyata direplikasi 365/730/1095 hari."""
    base = db.scalar("SELECT COUNT(*) FROM dw.fact_jadwal_snapshot WHERE snapshot_date_key = "
                     "(SELECT MAX(snapshot_date_key) FROM dw.fact_jadwal_snapshot)") or 0
    if not base:
        return
    for days in (365, 730, 1095):
        db.execute(f"""DROP TABLE IF EXISTS perf.jadwal_syn;
            CREATE TABLE perf.jadwal_syn AS
            SELECT TO_CHAR(DATE '2026-11-04' - d, 'YYYYMMDD')::int AS snapshot_date_key, j.perusahaan_key,
                   j.roket_key, j.hari_menuju_peluncuran + d AS hari_menuju_peluncuran, j.is_bergeser
            FROM dw.fact_jadwal_snapshot j CROSS JOIN generate_series(0, {days - 1}) d
            WHERE j.snapshot_date_key = (SELECT MAX(snapshot_date_key) FROM dw.fact_jadwal_snapshot);
            ANALYZE perf.jadwal_syn;""")
        n = db.scalar("SELECT COUNT(*) FROM perf.jadwal_syn")
        q = """SELECT perusahaan_key, COUNT(*) FROM perf.jadwal_syn
               WHERE snapshot_date_key = (SELECT MAX(snapshot_date_key) FROM perf.jadwal_syn)
                 AND hari_menuju_peluncuran BETWEEN 0 AND 90 GROUP BY 1"""
        _rec(run_label, "T6_snapshot_query", "no_index", days / 365, n, _time_query(q), f"{days} hari snapshot")
        db.execute("CREATE INDEX ON perf.jadwal_syn (snapshot_date_key); ANALYZE perf.jadwal_syn")
        _rec(run_label, "T6_snapshot_query", "with_index", days / 365, n, _time_query(q), f"{days} hari snapshot")
