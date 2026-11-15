-- dw_validation.sql : query validasi Data Warehouse setelah load (post-load quality checks)
-- Format: setiap blok diawali "-- check: <rule_id> | <dimensi> | <severity> | <nama>"
-- dan harus mengembalikan 2 kolom: failed, total. Dijalankan otomatis oleh
-- pipeline/quality/dw_checks.py (task validate_dw di DAG) -> hasil ke meta.dq_result.

-- check: RI-01 | integrity | ERROR | fact_peluncuran tanpa pasangan di dimensi (orphan FK)
SELECT COUNT(*) FILTER (WHERE t.date_key IS NULL OR p.perusahaan_key IS NULL OR r.roket_key IS NULL
                         OR l.lokasi_key IS NULL OR s.status_key IS NULL OR o.orbit_key IS NULL), COUNT(*)
FROM dw.fact_peluncuran f
LEFT JOIN dw.dim_tanggal t ON t.date_key = f.date_key
LEFT JOIN dw.dim_perusahaan p ON p.perusahaan_key = f.perusahaan_key
LEFT JOIN dw.dim_roket r ON r.roket_key = f.roket_key
LEFT JOIN dw.dim_lokasi l ON l.lokasi_key = f.lokasi_key
LEFT JOIN dw.dim_status s ON s.status_key = f.status_key
LEFT JOIN dw.dim_orbit o ON o.orbit_key = f.orbit_key;

-- check: RI-02 | integrity | ERROR | fact_jadwal_snapshot tanpa pasangan di dimensi (orphan FK)
SELECT COUNT(*) FILTER (WHERE t1.date_key IS NULL OR t2.date_key IS NULL OR p.perusahaan_key IS NULL
                         OR r.roket_key IS NULL OR l.lokasi_key IS NULL OR s.status_key IS NULL), COUNT(*)
FROM dw.fact_jadwal_snapshot f
LEFT JOIN dw.dim_tanggal t1 ON t1.date_key = f.snapshot_date_key
LEFT JOIN dw.dim_tanggal t2 ON t2.date_key = f.net_date_key
LEFT JOIN dw.dim_perusahaan p ON p.perusahaan_key = f.perusahaan_key
LEFT JOIN dw.dim_roket r ON r.roket_key = f.roket_key
LEFT JOIN dw.dim_lokasi l ON l.lokasi_key = f.lokasi_key
LEFT JOIN dw.dim_status s ON s.status_key = f.status_key;

-- check: RI-03 | integrity | ERROR | fact_konfigurasi_roket tanpa pasangan dim_roket / dim_tanggal
SELECT COUNT(*) FILTER (WHERE r.roket_key IS NULL OR t.date_key IS NULL), COUNT(*)
FROM dw.fact_konfigurasi_roket f
LEFT JOIN dw.dim_roket r ON r.roket_key = f.roket_key
LEFT JOIN dw.dim_tanggal t ON t.date_key = f.snapshot_date_key;

-- check: RI-04 | integrity | ERROR | peluncuran merujuk versi roket yang tidak berlaku pada tanggalnya (SCD2)
SELECT COUNT(*) FILTER (WHERE NOT (t.tanggal BETWEEN r.start_date AND r.end_date)), COUNT(*)
FROM dw.fact_peluncuran f JOIN dw.dim_roket r ON r.roket_key = f.roket_key
JOIN dw.dim_tanggal t ON t.date_key = f.date_key;

-- check: SCD-01 | consistency | ERROR | dim_roket: tepat satu versi aktif per roket dan periode tidak tumpang tindih
SELECT (SELECT COUNT(*) FROM (SELECT nama_roket FROM dw.dim_roket GROUP BY nama_roket
                              HAVING COUNT(*) FILTER (WHERE is_current) <> 1) x)
     + (SELECT COUNT(*) FROM dw.dim_roket a JOIN dw.dim_roket b ON a.nama_roket = b.nama_roket
        AND a.roket_key < b.roket_key AND a.start_date <= b.end_date AND b.start_date <= a.end_date),
       (SELECT COUNT(DISTINCT nama_roket) FROM dw.dim_roket);

-- check: UQ-01 | uniqueness | ERROR | natural key fact_peluncuran unik
SELECT COUNT(*) - COUNT(DISTINCT sumber_id), COUNT(*) FROM dw.fact_peluncuran;

-- check: UQ-02 | uniqueness | ERROR | grain fact_jadwal_snapshot (id LL2 + tanggal snapshot) unik
SELECT COUNT(*) - COUNT(DISTINCT (ll2_launch_id, snapshot_date_key)), COUNT(*) FROM dw.fact_jadwal_snapshot;

-- check: REC-01 | consistency | ERROR | rekonsiliasi jumlah baris staging clean vs fact_peluncuran
SELECT ABS((SELECT COUNT(*) FROM staging.stg_peluncuran_clean) - (SELECT COUNT(*) FROM dw.fact_peluncuran)),
       (SELECT COUNT(*) FROM staging.stg_peluncuran_clean);

-- check: REC-02 | consistency | ERROR | rekonsiliasi arsip internal = 4629 baris (4630 dikurangi 1 duplikat TC-01)
SELECT ABS(COUNT(*) - 4629), 4629 FROM dw.fact_peluncuran WHERE sumber_data = 'Arsip Internal';

-- check: CON-01 | consistency | ERROR | is_sukses = 1 hanya untuk status Success
SELECT COUNT(*) FILTER (WHERE (f.is_sukses = 1) <> (s.nama_status = 'Success')), COUNT(*)
FROM dw.fact_peluncuran f JOIN dw.dim_status s ON s.status_key = f.status_key;

-- check: CON-02 | consistency | ERROR | fact_peluncuran hanya berisi status kelompok Hasil (final)
SELECT COUNT(*) FILTER (WHERE s.kelompok_status <> 'Hasil'), COUNT(*)
FROM dw.fact_peluncuran f JOIN dw.dim_status s ON s.status_key = f.status_key;

-- check: CON-03 | consistency | ERROR | tidak ada peluncuran LL2 yang tumpang tindih periode arsip (TI-01)
SELECT COUNT(*) FILTER (WHERE f.sumber_data = 'Launch Library 2' AND t.tanggal < DATE '2022-07-29'), COUNT(*)
FROM dw.fact_peluncuran f JOIN dw.dim_tanggal t ON t.date_key = f.date_key;

-- check: CON-04 | consistency | WARN | statistik konfigurasi: sukses + gagal tidak melebihi total
SELECT COUNT(*) FILTER (WHERE peluncuran_sukses + peluncuran_gagal > total_peluncuran), COUNT(*)
FROM dw.fact_konfigurasi_roket;

-- check: RNG-01 | validity | ERROR | harga_musd positif dan tidak lebih dari 10000 juta USD
SELECT COUNT(*) FILTER (WHERE harga_musd <= 0 OR harga_musd > 10000), COUNT(harga_musd) FROM dw.fact_peluncuran;

-- check: RNG-02 | validity | ERROR | koordinat lokasi valid
SELECT COUNT(*) FILTER (WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180),
       COUNT(latitude) FROM dw.dim_lokasi;

-- check: RNG-03 | validity | WARN | jadwal pada snapshot terbaru dengan net lebih dari 1 hari di masa lalu
SELECT COUNT(*) FILTER (WHERE hari_menuju_peluncuran < -1), COUNT(*) FROM dw.fact_jadwal_snapshot
WHERE snapshot_date_key = (SELECT MAX(snapshot_date_key) FROM dw.fact_jadwal_snapshot);

-- check: CMP-01 | completeness | INFO | kelengkapan harga peluncuran (NULL tidak diimputasi)
SELECT COUNT(*) FILTER (WHERE harga_musd IS NULL), COUNT(*) FROM dw.fact_peluncuran;

-- check: CMP-02 | completeness | INFO | peluncuran tanpa data orbit (orbit_key = -1)
SELECT COUNT(*) FILTER (WHERE orbit_key = -1), COUNT(*) FROM dw.fact_peluncuran;

-- check: ACC-01 | accuracy | WARN | akurasi: jumlah peluncuran roket aktif di DWH vs statistik LL2 berselisih lebih dari 10%
WITH k AS (SELECT DISTINCT ON (ll2_config_id) ll2_config_id, roket_key, total_peluncuran
           FROM dw.fact_konfigurasi_roket ORDER BY ll2_config_id, snapshot_date_key DESC),
d AS (SELECT r.nama_roket, SUM(k.total_peluncuran) AS ll2_total
      FROM k JOIN dw.dim_roket r ON r.roket_key = k.roket_key
      WHERE r.is_current AND r.status_roket = 'Active' GROUP BY r.nama_roket HAVING SUM(k.total_peluncuran) >= 20),
f AS (SELECT r.nama_roket, COUNT(*) AS dw_total
      FROM dw.fact_peluncuran f JOIN dw.dim_roket r ON r.roket_key = f.roket_key GROUP BY r.nama_roket)
SELECT COUNT(*) FILTER (WHERE ABS(COALESCE(f.dw_total, 0) - d.ll2_total) > 0.10 * d.ll2_total), COUNT(*)
FROM d LEFT JOIN f ON f.nama_roket = d.nama_roket;

-- check: FRS-01 | timeliness | WARN | snapshot jadwal terbaru lebih dari 1 hari yang lalu
SELECT CASE WHEN MAX(snapshot_date_key) < TO_CHAR(CURRENT_DATE - 1, 'YYYYMMDD')::int THEN 1 ELSE 0 END, 1
FROM dw.fact_jadwal_snapshot;
