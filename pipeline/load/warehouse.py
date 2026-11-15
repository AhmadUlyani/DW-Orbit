"""Load staging clean -> Data Warehouse (schema dw).

Dimensi dimuat dulu supaya surrogate key tersedia, baru fakta. Setiap fungsi berjalan dalam satu
transaksi dan aman dijalankan ulang (upsert + unique constraint pada natural key).

  dim_perusahaan, dim_lokasi, dim_orbit : SCD Type 1 (upsert, nilai baru menimpa)
  dim_roket                              : SCD Type 2 untuk status_roket (Active/Inactive)
  fact_peluncuran                        : upsert inkremental (watermark transformed_at)
  fact_konfigurasi_roket                 : append snapshot mingguan (ON CONFLICT DO NOTHING)
  fact_jadwal_snapshot                   : append snapshot harian (ON CONFLICT DO UPDATE untuk reprocess)
"""
from datetime import date, timedelta

from .. import db
from ..ll2_client import WatermarkStore
from ..runlog import track_batch, write_lineage

HASIL = ("Success", "Failure", "Partial Failure", "Prelaunch Failure")

SQL_DIM_PERUSAHAAN = """
WITH src AS (
    SELECT perusahaan_nama AS n, perusahaan_jenis AS j, sumber_data AS s FROM staging.stg_peluncuran_clean
    UNION ALL SELECT perusahaan_nama, perusahaan_jenis, 'Launch Library 2' FROM staging.stg_jadwal_clean
    UNION ALL SELECT produsen_nama, produsen_jenis, 'Launch Library 2' FROM staging.stg_roket_clean
              WHERE produsen_nama IS NOT NULL),
agg AS (
    SELECT n, (ARRAY_AGG(j ORDER BY (j IS NULL), j))[1] AS j,
           CASE WHEN COUNT(DISTINCT s) > 1 THEN 'Keduanya' ELSE MIN(s) END AS s
    FROM src GROUP BY n)
INSERT INTO dw.dim_perusahaan AS d (nama_perusahaan, jenis_perusahaan, sumber_data)
SELECT n, j, s FROM agg
ON CONFLICT (nama_perusahaan) DO UPDATE SET
    jenis_perusahaan = COALESCE(EXCLUDED.jenis_perusahaan, d.jenis_perusahaan),
    sumber_data = CASE WHEN d.sumber_data IS NULL OR d.sumber_data = EXCLUDED.sumber_data
                       THEN EXCLUDED.sumber_data ELSE 'Keduanya' END,
    updated_at = now()
WHERE (d.jenis_perusahaan, d.sumber_data) IS DISTINCT FROM
      (COALESCE(EXCLUDED.jenis_perusahaan, d.jenis_perusahaan),
       CASE WHEN d.sumber_data IS NULL OR d.sumber_data = EXCLUDED.sumber_data
            THEN EXCLUDED.sumber_data ELSE 'Keduanya' END)
"""

SQL_DIM_LOKASI = """
WITH src AS (
    SELECT landasan, situs, wilayah, kode_negara, latitude, longitude FROM staging.stg_peluncuran_clean
    UNION ALL
    SELECT landasan, situs, wilayah, kode_negara, latitude, longitude FROM staging.stg_jadwal_clean),
agg AS (
    SELECT landasan, situs, kode_negara, MAX(wilayah) AS wilayah,
           ROUND(AVG(latitude), 6) AS latitude, ROUND(AVG(longitude), 6) AS longitude
    FROM src GROUP BY landasan, situs, kode_negara)
INSERT INTO dw.dim_lokasi AS d (landasan, situs, wilayah, kode_negara, latitude, longitude)
SELECT landasan, situs, wilayah, kode_negara, latitude, longitude FROM agg
ON CONFLICT (COALESCE(landasan, ''), situs, COALESCE(kode_negara, '---')) DO UPDATE SET
    wilayah   = COALESCE(d.wilayah, EXCLUDED.wilayah),
    latitude  = COALESCE(EXCLUDED.latitude, d.latitude),
    longitude = COALESCE(EXCLUDED.longitude, d.longitude),
    updated_at = now()
WHERE (d.wilayah, d.latitude, d.longitude) IS DISTINCT FROM
      (COALESCE(d.wilayah, EXCLUDED.wilayah), COALESCE(EXCLUDED.latitude, d.latitude),
       COALESCE(EXCLUDED.longitude, d.longitude))
"""

SQL_DIM_ORBIT = """
WITH src AS (
    SELECT orbit_singkatan AS a, orbit_nama AS n FROM staging.stg_peluncuran_clean
    UNION ALL SELECT orbit_singkatan, orbit_nama FROM staging.stg_jadwal_clean),
agg AS (SELECT a, MAX(n) AS n FROM src WHERE a IS NOT NULL AND a <> 'N/A' GROUP BY a)
INSERT INTO dw.dim_orbit AS d (orbit_key, singkatan, nama_orbit, kelompok_orbit)
SELECT nextval('dw.seq_orbit_key'), agg.a, COALESCE(agg.n, agg.a), COALESCE(m.kelompok_orbit, 'Lainnya')
FROM agg LEFT JOIN meta.map_orbit m ON m.singkatan = agg.a
WHERE NOT EXISTS (SELECT 1 FROM dw.dim_orbit x WHERE x.singkatan = agg.a)
"""

SQL_DIM_ORBIT_UPD = """
UPDATE dw.dim_orbit d SET kelompok_orbit = m.kelompok_orbit, updated_at = now()
FROM meta.map_orbit m WHERE m.singkatan = d.singkatan AND d.kelompok_orbit <> m.kelompok_orbit
"""

SQL_DIM_STATUS = """
INSERT INTO dw.dim_status (nama_status, kelompok_status, is_final)
SELECT DISTINCT s, CASE WHEN s = ANY(%(hasil)s) THEN 'Hasil' ELSE 'Jadwal' END, s = ANY(%(hasil)s)
FROM (SELECT status_nama s FROM staging.stg_peluncuran_clean
      UNION SELECT status_nama FROM staging.stg_jadwal_clean) x
ON CONFLICT (nama_status) DO NOTHING
"""


def load_dimensions(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "load_dimensions", "staging") as b:
        counts = {}
        with db.transaction() as conn, conn.cursor() as cur:
            for name, sql, params in [("dim_perusahaan", SQL_DIM_PERUSAHAAN, None),
                                      ("dim_lokasi", SQL_DIM_LOKASI, None),
                                      ("dim_orbit", SQL_DIM_ORBIT, None),
                                      ("dim_orbit_upd", SQL_DIM_ORBIT_UPD, None),
                                      ("dim_status", SQL_DIM_STATUS, {"hasil": list(HASIL)})]:
                cur.execute(sql, params)
                counts[name] = cur.rowcount
            counts["dim_roket"] = _scd2_roket(cur)
        b.rows_written = sum(counts.values())
        b.note(f"upsert {counts}")
        for t in ("dim_perusahaan", "dim_lokasi", "dim_orbit", "dim_status", "dim_roket"):
            write_lineage(b.batch_id, f"dw.{t}", "dw",
                          ["staging.stg_peluncuran_clean", "staging.stg_jadwal_clean", "staging.stg_roket_clean"],
                          ["SCD2" if t == "dim_roket" else "SCD1"], counts.get(t, 0))
        db.execute("ANALYZE dw.dim_perusahaan; ANALYZE dw.dim_lokasi; ANALYZE dw.dim_orbit; ANALYZE dw.dim_roket;")
        return counts


def _scd2_roket(cur):
    """SCD Type 2 dim_roket.

    - Roket baru: versi pertama berlaku sejak 1957-01-01. Status awal = status arsip (per Agustus 2022)
      bila roket ada di arsip, selain itu status LL2.
    - Status LL2 terbaru (snapshot konfigurasi) dibandingkan versi berlaku. Bila berbeda: versi lama ditutup
      (end_date = snapshot - 1 hari, is_current = false) dan versi baru ditambahkan mulai tanggal snapshot.
    """
    cur.execute("""SELECT roket_nama, MAX(roket_status) FILTER (WHERE sumber_data='Arsip Internal'),
                          MIN(tanggal) FROM staging.stg_peluncuran_clean GROUP BY roket_nama""")
    arsip = {n: (st, t) for n, st, t in cur.fetchall()}
    cur.execute("""SELECT DISTINCT ON (roket_nama) roket_nama, ll2_config_id, status_roket, snapshot_date
                   FROM (SELECT roket_nama, ll2_config_id, snapshot_date, total_peluncuran,
                                CASE WHEN BOOL_OR(status_roket='Active') OVER w THEN 'Active' ELSE 'Inactive' END
                                     AS status_roket
                         FROM staging.stg_roket_clean
                         WINDOW w AS (PARTITION BY roket_nama, snapshot_date)) x
                   ORDER BY roket_nama, snapshot_date DESC, total_peluncuran DESC NULLS LAST""")
    ll2 = {n: (cid, st, snap) for n, cid, st, snap in cur.fetchall()}
    cur.execute("SELECT DISTINCT roket_nama FROM staging.stg_jadwal_clean")
    names = set(arsip) | set(ll2) | {r[0] for r in cur.fetchall()}
    cur.execute("SELECT roket_key, nama_roket, status_roket, start_date, ll2_config_id FROM dw.dim_roket WHERE is_current")
    current = {n: (k, st, sd, cid) for k, n, st, sd, cid in cur.fetchall()}
    changes = 0
    for n in sorted(names):
        cid, st_ll2, snap = ll2.get(n, (None, None, None))
        if n not in current:
            st0 = (arsip.get(n, (None,))[0]) or st_ll2 or "Active"
            cur.execute("""INSERT INTO dw.dim_roket (nama_roket, ll2_config_id, status_roket, start_date)
                           VALUES (%s,%s,%s,'1957-01-01') RETURNING roket_key""", (n, cid, st0))
            current[n] = (cur.fetchone()[0], st0, date(1957, 1, 1), cid)
            changes += 1
        key, st_cur, start, cid_cur = current[n]
        if cid is not None and cid_cur is None:                                  # atribut Type 1
            cur.execute("UPDATE dw.dim_roket SET ll2_config_id=%s WHERE roket_key=%s", (cid, key))
        if st_ll2 and st_ll2 != st_cur and snap and snap > start:               # perubahan Type 2
            cur.execute("UPDATE dw.dim_roket SET end_date=%s, is_current=FALSE WHERE roket_key=%s",
                        (snap - timedelta(days=1), key))
            cur.execute("""INSERT INTO dw.dim_roket (nama_roket, ll2_config_id, status_roket, start_date)
                           VALUES (%s,%s,%s,%s)""", (n, cid or cid_cur, st_ll2, snap))
            changes += 1
    return changes


SQL_FACT_PELUNCURAN = """
INSERT INTO dw.fact_peluncuran AS f (sumber_id, date_key, perusahaan_key, roket_key, lokasi_key, status_key,
    orbit_key, waktu_peluncuran, waktu_diketahui, nama_misi, jenis_misi, alasan_gagal, harga_musd,
    sumber_harga, is_sukses, sumber_data, batch_id)
SELECT c.sumber_id, TO_CHAR(c.tanggal, 'YYYYMMDD')::int, p.perusahaan_key, r.roket_key, l.lokasi_key,
       s.status_key, COALESCE(o.orbit_key, -1), c.waktu, c.waktu_diketahui, c.nama_misi, c.jenis_misi,
       c.alasan_gagal, c.harga_musd, c.sumber_harga, c.is_sukses, c.sumber_data, %(batch)s
FROM staging.stg_peluncuran_clean c
JOIN dw.dim_perusahaan p ON p.nama_perusahaan = c.perusahaan_nama
JOIN dw.dim_roket r      ON r.nama_roket = c.roket_nama AND c.tanggal BETWEEN r.start_date AND r.end_date
JOIN dw.dim_lokasi l     ON COALESCE(l.landasan, '') = COALESCE(c.landasan, '') AND l.situs = c.situs
                        AND COALESCE(l.kode_negara, '---') = COALESCE(c.kode_negara, '---')
JOIN dw.dim_status s     ON s.nama_status = c.status_nama
LEFT JOIN dw.dim_orbit o ON o.singkatan = c.orbit_singkatan
WHERE c.transformed_at > %(wm)s
ON CONFLICT (sumber_id) DO UPDATE SET
    date_key=EXCLUDED.date_key, perusahaan_key=EXCLUDED.perusahaan_key, roket_key=EXCLUDED.roket_key,
    lokasi_key=EXCLUDED.lokasi_key, status_key=EXCLUDED.status_key, orbit_key=EXCLUDED.orbit_key,
    waktu_peluncuran=EXCLUDED.waktu_peluncuran, waktu_diketahui=EXCLUDED.waktu_diketahui,
    nama_misi=EXCLUDED.nama_misi, jenis_misi=EXCLUDED.jenis_misi, alasan_gagal=EXCLUDED.alasan_gagal,
    harga_musd=EXCLUDED.harga_musd, sumber_harga=EXCLUDED.sumber_harga, is_sukses=EXCLUDED.is_sukses,
    batch_id=EXCLUDED.batch_id, loaded_at=now()
"""

SQL_FACT_KONFIGURASI = """
INSERT INTO dw.fact_konfigurasi_roket (snapshot_date_key, roket_key, produsen_key, ll2_config_id,
    perkiraan_biaya_musd, total_peluncuran, peluncuran_sukses, peluncuran_gagal, sukses_beruntun,
    peluncuran_terjadwal, batch_id)
SELECT TO_CHAR(c.snapshot_date, 'YYYYMMDD')::int, r.roket_key, p.perusahaan_key, c.ll2_config_id,
       c.perkiraan_biaya_musd, c.total_peluncuran, c.peluncuran_sukses, c.peluncuran_gagal,
       c.sukses_beruntun, c.peluncuran_terjadwal, %(batch)s
FROM staging.stg_roket_clean c
JOIN dw.dim_roket r ON r.nama_roket = c.roket_nama AND c.snapshot_date BETWEEN r.start_date AND r.end_date
LEFT JOIN dw.dim_perusahaan p ON p.nama_perusahaan = c.produsen_nama
WHERE c.is_full_snapshot
ON CONFLICT (ll2_config_id, snapshot_date_key) DO NOTHING
"""

SQL_FACT_JADWAL = """
INSERT INTO dw.fact_jadwal_snapshot AS f (snapshot_date_key, net_date_key, ll2_launch_id, perusahaan_key,
    roket_key, lokasi_key, status_key, orbit_key, net_waktu, window_start, window_end, presisi_jadwal,
    nama_misi, jenis_misi, hari_menuju_peluncuran, geser_jam, is_bergeser, batch_id)
SELECT TO_CHAR(c.snapshot_date, 'YYYYMMDD')::int, TO_CHAR(c.net_waktu AT TIME ZONE 'UTC', 'YYYYMMDD')::int,
       c.ll2_launch_id, p.perusahaan_key, r.roket_key, l.lokasi_key, s.status_key, COALESCE(o.orbit_key, -1),
       c.net_waktu, c.window_start, c.window_end, c.presisi_jadwal, c.nama_misi, c.jenis_misi,
       c.hari_menuju_peluncuran, c.geser_jam, c.is_bergeser, %(batch)s
FROM staging.stg_jadwal_clean c
JOIN dw.dim_perusahaan p ON p.nama_perusahaan = c.perusahaan_nama
JOIN dw.dim_roket r      ON r.nama_roket = c.roket_nama AND c.snapshot_date BETWEEN r.start_date AND r.end_date
JOIN dw.dim_lokasi l     ON COALESCE(l.landasan, '') = COALESCE(c.landasan, '') AND l.situs = c.situs
                        AND COALESCE(l.kode_negara, '---') = COALESCE(c.kode_negara, '---')
JOIN dw.dim_status s     ON s.nama_status = c.status_nama
LEFT JOIN dw.dim_orbit o ON o.singkatan = c.orbit_singkatan
WHERE c.transformed_at > %(wm)s
ON CONFLICT (ll2_launch_id, snapshot_date_key) DO UPDATE SET
    net_date_key=EXCLUDED.net_date_key, perusahaan_key=EXCLUDED.perusahaan_key, roket_key=EXCLUDED.roket_key,
    lokasi_key=EXCLUDED.lokasi_key, status_key=EXCLUDED.status_key, orbit_key=EXCLUDED.orbit_key,
    net_waktu=EXCLUDED.net_waktu, window_start=EXCLUDED.window_start, window_end=EXCLUDED.window_end,
    presisi_jadwal=EXCLUDED.presisi_jadwal, hari_menuju_peluncuran=EXCLUDED.hari_menuju_peluncuran,
    geser_jam=EXCLUDED.geser_jam, is_bergeser=EXCLUDED.is_bergeser, batch_id=EXCLUDED.batch_id, loaded_at=now()
"""


def _load_fact(cur, sql, batch_id, wm_name, src_table):
    wm = WatermarkStore.get(wm_name, "1900-01-01")
    cur.execute(f"SELECT COUNT(*), MAX(transformed_at) FROM {src_table} WHERE transformed_at > %s", (wm,))
    n_src, new_wm = cur.fetchone()
    cur.execute(sql, {"batch": batch_id, "wm": wm})
    return n_src, cur.rowcount, new_wm


def load_facts(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "load_facts", "staging") as b:
        res = {}
        with db.transaction() as conn, conn.cursor() as cur:
            res["fact_peluncuran"] = _load_fact(cur, SQL_FACT_PELUNCURAN, b.batch_id, "load_fact_peluncuran",
                                                "staging.stg_peluncuran_clean")
            res["fact_jadwal_snapshot"] = _load_fact(cur, SQL_FACT_JADWAL, b.batch_id, "load_fact_jadwal",
                                                     "staging.stg_jadwal_clean")
            cur.execute(SQL_FACT_KONFIGURASI, {"batch": b.batch_id})
            res["fact_konfigurasi_roket"] = (None, cur.rowcount, None)
            # rekonsiliasi: kalau ada baris staging yang gagal join dimensi, transaksi dibatalkan
            for t, (n_src, n_loaded, _) in res.items():
                if n_src is not None and n_loaded < n_src:
                    raise RuntimeError(f"{t}: hanya {n_loaded} dari {n_src} baris staging termuat "
                                       f"(lookup dimensi gagal) -> rollback")
        # watermark diperbarui setelah commit
        for wm_name, key in (("load_fact_peluncuran", "fact_peluncuran"), ("load_fact_jadwal", "fact_jadwal_snapshot")):
            if res[key][2] is not None:
                WatermarkStore.set(wm_name, res[key][2].isoformat())
        db.execute("ANALYZE dw.fact_peluncuran; ANALYZE dw.fact_jadwal_snapshot; ANALYZE dw.fact_konfigurasi_roket;")
        b.rows_read = sum(v[0] or 0 for v in res.values())
        b.rows_written = sum(v[1] for v in res.values())
        b.note(", ".join(f"{k}: {v[1]} baris" for k, v in res.items()))
        for t, (_, n, _) in res.items():
            src = {"fact_peluncuran": "staging.stg_peluncuran_clean", "fact_jadwal_snapshot": "staging.stg_jadwal_clean",
                   "fact_konfigurasi_roket": "staging.stg_roket_clean"}[t]
            write_lineage(b.batch_id, f"dw.{t}", "dw", [src], ["SURROGATE-KEY-LOOKUP", "UPSERT"], n)
        return {k: v[1] for k, v in res.items()}
