-- 04_mart.sql : DATA MART (view di atas schema dw) -> sumber dashboard BI
--   Data Mart Peluncuran Historis : v_peluncuran, v_keandalan_tahunan, v_biaya_roket
--   Data Mart Jadwal Peluncuran   : v_jadwal_terkini, v_pergeseran_jadwal
--   Monitoring (read-only, tanpa membuka schema meta ke BI): v_pipeline_*, v_kualitas_data

CREATE OR REPLACE VIEW mart.v_peluncuran AS
SELECT f.peluncuran_key, f.sumber_id, t.tanggal, t.tahun, t.bulan, t.kuartal, t.dekade,
       p.nama_perusahaan, p.jenis_perusahaan,
       r.nama_roket, r.status_roket, r.ll2_config_id,
       l.landasan, l.situs, l.wilayah, l.kode_negara, l.latitude, l.longitude,
       o.singkatan AS orbit, o.nama_orbit, o.kelompok_orbit,
       s.nama_status AS status_misi, f.is_sukses,
       CASE WHEN f.is_sukses = 0 THEN 1 ELSE 0 END AS is_gagal,
       f.waktu_peluncuran, f.waktu_diketahui, f.nama_misi, f.jenis_misi, f.alasan_gagal,
       f.harga_musd, f.sumber_harga, f.sumber_data
FROM dw.fact_peluncuran f
JOIN dw.dim_tanggal    t ON t.date_key = f.date_key
JOIN dw.dim_perusahaan p ON p.perusahaan_key = f.perusahaan_key
JOIN dw.dim_roket      r ON r.roket_key = f.roket_key
JOIN dw.dim_lokasi     l ON l.lokasi_key = f.lokasi_key
JOIN dw.dim_status     s ON s.status_key = f.status_key
JOIN dw.dim_orbit      o ON o.orbit_key = f.orbit_key;

CREATE OR REPLACE VIEW mart.v_keandalan_tahunan AS
SELECT t.tahun, p.nama_perusahaan, p.jenis_perusahaan,
       COUNT(*)                                             AS jumlah_peluncuran,
       SUM(f.is_sukses)                                     AS sukses,
       SUM(CASE WHEN s.nama_status = 'Failure' THEN 1 ELSE 0 END)           AS failure,
       SUM(CASE WHEN s.nama_status = 'Partial Failure' THEN 1 ELSE 0 END)   AS partial_failure,
       SUM(CASE WHEN s.nama_status = 'Prelaunch Failure' THEN 1 ELSE 0 END) AS prelaunch_failure,
       ROUND(100.0 * SUM(f.is_sukses) / COUNT(*), 2)        AS tingkat_keberhasilan
FROM dw.fact_peluncuran f
JOIN dw.dim_tanggal t    ON t.date_key = f.date_key
JOIN dw.dim_perusahaan p ON p.perusahaan_key = f.perusahaan_key
JOIN dw.dim_status s     ON s.status_key = f.status_key
GROUP BY t.tahun, p.nama_perusahaan, p.jenis_perusahaan;

-- snapshot konfigurasi roket terbaru + rekam jejak dari fact_peluncuran
CREATE OR REPLACE VIEW mart.v_biaya_roket AS
WITH latest AS (SELECT MAX(snapshot_date_key) AS k FROM dw.fact_konfigurasi_roket),
hist AS (
    SELECT r.nama_roket, COUNT(*) n, SUM(f.is_sukses) s,
           MAX(t.tanggal) terakhir, AVG(f.harga_musd) harga_rata_arsip
    FROM dw.fact_peluncuran f
    JOIN dw.dim_roket r   ON r.roket_key = f.roket_key
    JOIN dw.dim_tanggal t ON t.date_key = f.date_key
    GROUP BY r.nama_roket)
SELECT k.ll2_config_id, r.nama_roket, r.status_roket,
       pr.nama_perusahaan AS produsen, pr.jenis_perusahaan AS jenis_produsen,
       k.perkiraan_biaya_musd, k.total_peluncuran, k.peluncuran_sukses, k.peluncuran_gagal,
       k.sukses_beruntun, k.peluncuran_terjadwal,
       CASE WHEN k.total_peluncuran > 0
            THEN ROUND(100.0 * k.peluncuran_sukses / k.total_peluncuran, 2) END AS tingkat_keberhasilan_ll2,
       h.n AS peluncuran_di_dw, h.terakhir AS peluncuran_terakhir_dw,
       ROUND(h.harga_rata_arsip, 2) AS harga_rata_arsip_musd,
       t.tanggal AS snapshot_tanggal
FROM dw.fact_konfigurasi_roket k
JOIN latest ON latest.k = k.snapshot_date_key
JOIN dw.dim_roket r ON r.roket_key = k.roket_key
LEFT JOIN dw.dim_perusahaan pr ON pr.perusahaan_key = k.produsen_key
LEFT JOIN hist h ON h.nama_roket = r.nama_roket
JOIN dw.dim_tanggal t ON t.date_key = k.snapshot_date_key;

-- tren biaya roket antar snapshot mingguan
CREATE OR REPLACE VIEW mart.v_biaya_roket_tren AS
SELECT t.tanggal AS snapshot_tanggal, r.nama_roket, k.perkiraan_biaya_musd,
       k.total_peluncuran, k.peluncuran_sukses, k.peluncuran_gagal
FROM dw.fact_konfigurasi_roket k
JOIN dw.dim_roket r   ON r.roket_key = k.roket_key
JOIN dw.dim_tanggal t ON t.date_key = k.snapshot_date_key;

-- jadwal pada snapshot terbaru + rekam jejak penyedia (5 tahun) + biaya + skor risiko ML
CREATE OR REPLACE VIEW mart.v_jadwal_terkini AS
WITH snap AS (SELECT MAX(snapshot_date_key) AS k FROM dw.fact_jadwal_snapshot),
rekam AS (
    SELECT f.perusahaan_key, COUNT(*) n_5th, SUM(f.is_sukses) s_5th
    FROM dw.fact_peluncuran f JOIN dw.dim_tanggal t ON t.date_key = f.date_key
    WHERE t.tanggal >= (CURRENT_DATE - INTERVAL '5 years')
    GROUP BY f.perusahaan_key),
biaya AS (
    SELECT DISTINCT ON (ll2_config_id) ll2_config_id, perkiraan_biaya_musd
    FROM dw.fact_konfigurasi_roket ORDER BY ll2_config_id, snapshot_date_key DESC),
geser AS (
    SELECT ll2_launch_id, COUNT(*) FILTER (WHERE is_bergeser = 1) AS kali_bergeser,
           SUM(geser_jam) FILTER (WHERE is_bergeser = 1) AS total_geser_jam,
           COUNT(*) AS jumlah_snapshot
    FROM dw.fact_jadwal_snapshot GROUP BY ll2_launch_id)
SELECT j.ll2_launch_id, ts.tanggal AS snapshot_tanggal, j.net_waktu, tn.tanggal AS tanggal_net,
       j.hari_menuju_peluncuran, j.presisi_jadwal, s.nama_status AS status_jadwal,
       p.nama_perusahaan, p.jenis_perusahaan, r.nama_roket, r.ll2_config_id,
       l.situs, l.landasan, l.kode_negara, l.latitude, l.longitude,
       o.singkatan AS orbit, o.nama_orbit, o.kelompok_orbit,
       j.nama_misi, j.jenis_misi, j.geser_jam, j.is_bergeser,
       g.kali_bergeser, g.total_geser_jam, g.jumlah_snapshot,
       rk.n_5th AS peluncuran_5th,
       CASE WHEN rk.n_5th > 0 THEN ROUND(100.0 * rk.s_5th / rk.n_5th, 2) END AS keberhasilan_5th,
       b.perkiraan_biaya_musd,
       pr.p_gagal, pr.risk_band, pr.faktor_utama, pr.model_version
FROM dw.fact_jadwal_snapshot j
JOIN snap ON snap.k = j.snapshot_date_key
JOIN dw.dim_tanggal ts   ON ts.date_key = j.snapshot_date_key
JOIN dw.dim_tanggal tn   ON tn.date_key = j.net_date_key
JOIN dw.dim_status s     ON s.status_key = j.status_key
JOIN dw.dim_perusahaan p ON p.perusahaan_key = j.perusahaan_key
JOIN dw.dim_roket r      ON r.roket_key = j.roket_key
JOIN dw.dim_lokasi l     ON l.lokasi_key = j.lokasi_key
JOIN dw.dim_orbit o      ON o.orbit_key = j.orbit_key
LEFT JOIN rekam rk ON rk.perusahaan_key = j.perusahaan_key
LEFT JOIN biaya b  ON b.ll2_config_id = r.ll2_config_id
LEFT JOIN geser g  ON g.ll2_launch_id = j.ll2_launch_id
LEFT JOIN ml.pred_launch_risk pr ON pr.ll2_launch_id = j.ll2_launch_id AND pr.snapshot_date = ts.tanggal;

-- pergeseran jadwal per penyedia per snapshot
CREATE OR REPLACE VIEW mart.v_pergeseran_jadwal AS
SELECT ts.tanggal AS snapshot_tanggal, p.nama_perusahaan,
       COUNT(*) AS jumlah_jadwal,
       COUNT(*) FILTER (WHERE j.geser_jam IS NOT NULL) AS jadwal_terbanding,
       COUNT(*) FILTER (WHERE j.is_bergeser = 1)       AS jadwal_bergeser,
       ROUND(AVG(j.geser_jam) FILTER (WHERE j.is_bergeser = 1), 2) AS rata_geser_jam
FROM dw.fact_jadwal_snapshot j
JOIN dw.dim_tanggal ts   ON ts.date_key = j.snapshot_date_key
JOIN dw.dim_perusahaan p ON p.perusahaan_key = j.perusahaan_key
GROUP BY ts.tanggal, p.nama_perusahaan;

-- monitoring & governance
CREATE OR REPLACE VIEW mart.v_pipeline_run AS
SELECT batch_id, pipeline, task, source, status, started_at, finished_at, duration_s,
       rows_read, rows_written, rows_rejected, api_requests, attempt, LEFT(message, 300) AS message
FROM meta.etl_batch;

CREATE OR REPLACE VIEW mart.v_kualitas_data AS
SELECT d.batch_id, d.layer, d.table_name, d.rule_id, d.check_name, d.dimension, d.severity,
       d.status, d.total_rows, d.failed_rows, d.fail_pct, d.threshold_pct, d.details, d.checked_at
FROM meta.dq_result d;

CREATE OR REPLACE VIEW mart.v_quality_gate AS
SELECT * FROM meta.quality_gate;

CREATE OR REPLACE VIEW mart.v_reject_ringkas AS
SELECT source, target_table, rule_id, reason, COUNT(*) AS jumlah, MAX(rejected_at) AS terakhir,
       BOOL_AND(resolved) AS semua_resolved
FROM staging.stg_reject GROUP BY source, target_table, rule_id, reason;

CREATE OR REPLACE VIEW mart.v_lake_ringkas AS
SELECT zone, dataset, COUNT(*) AS objek, SUM(size_bytes) AS total_bytes,
       SUM(record_count) AS total_record, MIN(ingest_date) AS dari, MAX(ingest_date) AS sampai
FROM meta.lake_object GROUP BY zone, dataset;

CREATE OR REPLACE VIEW mart.v_backup AS
SELECT backup_id, backup_type, object_key, size_bytes, objects_copied, status,
       started_at, finished_at, retention_until FROM meta.backup_log;

CREATE OR REPLACE VIEW mart.v_restore_test AS SELECT * FROM meta.restore_test;
CREATE OR REPLACE VIEW mart.v_perf_result AS SELECT * FROM meta.perf_result;
CREATE OR REPLACE VIEW mart.v_alert AS SELECT * FROM meta.alert;
CREATE OR REPLACE VIEW mart.v_data_catalog AS SELECT * FROM meta.data_catalog;
CREATE OR REPLACE VIEW mart.v_lineage AS SELECT * FROM meta.lineage;
