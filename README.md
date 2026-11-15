# Orbit Launch Intelligence: Big Data Platform & Data Warehouse

UAS Kecerdasan Bisnis (bidang Big Data), lanjutan rancangan UTS *"Desain Data Warehouse Data Peluncuran Roket pada
PT Orbit Nusantara Analitika"*.

```
Business Problem → Big Data Sources → Data Ingestion → Data Lake → ETL/ELT → Orchestration
→ Data Warehouse/Data Mart → Machine Learning → Evaluation → Dashboard → Insight → Recommendation
```

| Komponen | Teknologi | Lokasi di repo |
|---|---|---|
| Sumber internal | `space_missions.csv` (Maven Analytics, 4.630 baris, 1957–2022) | `data/source/` |
| Sumber eksternal | Launch Library 2 API v2.3.0 (The Space Devs), JSON, 3 endpoint | `pipeline/ll2_client.py` |
| Data Lake | MinIO (S3-compatible): `orbit-raw`, `orbit-clean`, `orbit-curated`, `orbit-backup` | `scripts/minio_init.sh`, `pipeline/lake.py` |
| Ingestion | Python (requests, ThrottleGuard 15 req/jam, checkpoint paging, checksum) | `pipeline/ingest/` |
| ETL & quality gate | Python + SQL (pandas, ftfy) | `pipeline/transform/`, `pipeline/quality/`, `pipeline/load/` |
| Orkestrasi | Apache Airflow 2.10 (LocalExecutor) | `dags/` |
| Data Warehouse | PostgreSQL 16, galaxy schema 6 dimensi + 3 fakta | `sql/ddl/` |
| Validasi query | 20 query validasi pasca-load | `sql/validation/dw_validation.sql` |
| Machine Learning | scikit-learn (klasifikasi risiko gagal + forecasting) | `pipeline/ml/` |
| Dashboard | Streamlit + Plotly (hanya membaca schema `mart` & `ml`) | `dashboard/` |
| Backup & DR | pg_dump + mirror raw zone + uji restore otomatis | `pipeline/ops/backup.py` |
| Uji skalabilitas | data sintetis berlabel di schema `perf` | `pipeline/ops/perf_test.py` |

## 1. Arsitektur

```
┌──────────────────┐   ingest (as-is)   ┌──────────────── Data Lake (MinIO) ─────────────────┐
│ CSV arsip (Maven)│──────────────────▶ │ orbit-raw     source=/dataset=/ingest_date=/batch= │
│ LL2 API (JSON)   │  checksum, retry,  │ orbit-clean   parquet (silver)                     │
│  /launches/prev  │  throttle, resume  │ orbit-curated parquet mart + model.joblib (gold)   │
│  /launches/upcom │                    │ orbit-backup  pg_dump + mirror raw (DR)            │
│  /launcher_conf  │                    └───────────────┬────────────────────────────────────┘
└──────────────────┘                                    │ stage (raw jsonb/text)
                                                        ▼
        PostgreSQL dw_orbit:  staging (raw → clean + stg_reject) ──quality gate──▶ dw (galaxy schema)
                              meta (log, lineage, DQ, watermark)                    │
                              mart (view data mart) ◀───────────────────────────────┤
                              ml   (model, prediksi, forecast, drift) ◀── scikit-learn
                                                        │
                                   Streamlit dashboard (user read-only bi_reader)
                     Semua langkah diorkestrasi Airflow; alert → meta.alert + webhook Discord
```

### Schema Data Warehouse (galaxy schema, Kimball)

| Tabel | Jenis | Grain |
|---|---|---|
| `dw.fact_peluncuran` | transaction fact | 1 baris = 1 peluncuran |
| `dw.fact_konfigurasi_roket` | periodic snapshot mingguan | 1 baris = 1 konfigurasi roket per minggu |
| `dw.fact_jadwal_snapshot` | periodic snapshot harian | 1 baris = 1 jadwal per tanggal snapshot |
| `dw.dim_tanggal` | role-playing | 1 hari (1957–2040, 30.681 baris) |
| `dw.dim_perusahaan`, `dim_lokasi`, `dim_orbit` | SCD Type 1 | 1 entitas |
| `dw.dim_roket` | SCD Type 2 (status Active/Inactive) | 1 versi status roket |
| `dw.dim_status` | statis | 1 status (kelompok Hasil / Jadwal) |

Semua dimensi memakai surrogate key. Fakta memakai FK dan unique constraint pada natural key, jadi load bisa diulang tanpa duplikasi.

## 2. Menjalankan proyek

### Prasyarat
Docker Desktop (sekitar 4 GB RAM bebas). Port: 5433 (PostgreSQL), 9000/9001 (MinIO), 8081 (Airflow), 8501 (dashboard).

### Langkah
```bash
cp .env.example .env              # lalu ganti semua CHANGE_ME (jangan commit .env)
docker compose up -d --build      # postgres, minio(+init bucket), airflow, dashboard
```
1. Buka Airflow di http://localhost:8081 (user/password dari `.env`).
2. Unpause lalu trigger DAG `orbit_init`. DAG ini membuat schema, memuat CSV dan LL2 (sekitar 24 request API; kalau
   kuota 15 request/jam habis, ia menunggu lalu lanjut dari checkpoint), load DWH, validasi, lalu memicu
   `orbit_ml_training`.
3. Unpause DAG terjadwal: `orbit_daily_etl`, `orbit_weekly_config`, `orbit_backup_dr`, `orbit_ml_training`.
4. Dashboard ada di http://localhost:8501, MinIO console di http://localhost:9001.

Menjalankan satu langkah pipeline tanpa Airflow (debug):
```bash
docker compose run --rm --no-deps airflow-scheduler python -c "from pipeline.quality import dw_checks; dw_checks.validate_dw()"
```

### Jadwal DAG (WITA / UTC+8)

Airflow, PostgreSQL, dan guard harian memakai zona WITA (`Asia/Makassar`), sama dengan jam laptop, jadi ekspresi
cron dibaca langsung sebagai jam WITA. Jadwalnya disesuaikan dengan jam laptop biasanya menyala.

| DAG | Jadwal | Isi |
|---|---|---|
| `orbit_backup_dr` | setiap hari 12:00 (`0 12 * * *`) | pg_dump → MinIO + `./backups`, mirror raw zone, retensi 30 hari, uji restore tiap Minggu |
| `orbit_daily_etl` | setiap hari 12:30 (`30 12 * * *`) | ingest CSV (checksum) + LL2 upcoming & previous inkremental → stage → transform + quality gate → load → validasi → export parquet → skor ML |
| `orbit_weekly_config` | Minggu 13:30 (`30 13 * * 0`) | snapshot penuh konfigurasi roket → SCD2 `dim_roket` + `fact_konfigurasi_roket` |
| `orbit_ml_training` | Senin 14:00 (`0 14 * * 1`) | training & evaluasi model, drift PSI, scoring jadwal |
| `orbit_init` / `orbit_reprocess` / `orbit_perf_test` | manual | muatan awal / rebuild dari Data Lake / uji skalabilitas |

**Run susulan.** Semua DAG memakai `catchup=False`. Kalau laptop baru menyala setelah jadwal lewat, scheduler membuat
satu run susulan untuk jadwal terakhir saja. DAG harian diawali task `guard_once_per_day` yang men-skip run kalau
`load_facts` sudah sukses di hari (WITA) yang sama. Contoh: laptop menyala pukul 11:00, run susulan kemarin jalan dan
sukses, lalu jadwal 12:30 hari itu di-skip. Supaya ini berjalan, aktifkan *Start Docker Desktop when you sign in*
(container memakai `restart: unless-stopped`). Trigger manual dari UI Airflow selalu melewati guard.

DAG mingguan memakai task `guard_once_per_week`: run terjadwal di-skip kalau `fact_konfigurasi_roket` sudah punya
snapshot di minggu ISO yang sama, misalnya dari run susulan setelah DAG di-unpause atau dari `orbit_init`/`orbit_reprocess`.
Grain tetap satu snapshot per minggu dan kuota API tidak terbuang. Trigger manual selalu jalan.

## 3. Keandalan pipeline (error handling, retry, alert, recovery)

| Kejadian | Penanganan |
|---|---|
| API timeout / HTTP 429 / 5xx | `ApiClient` retry 3× dengan backoff 10s→30s→90s; Airflow retry task 3× (2→4→8 menit) |
| Kuota API habis | `ThrottleGuard` cek `/api-throttle/` sebelum tiap request dan menunggu; `PagedFetcher` menyimpan checkpoint per halaman sehingga proses lanjut dari halaman terakhir |
| Struktur JSON / kolom CSV berubah | `SchemaChanged` / `SchemaError`: data tidak ditulis; raw tetap di Data Lake untuk diproses ulang |
| Data corrupt | checksum SHA-256 objek lake diverifikasi sebelum staging |
| Baris tidak valid | dipindah ke `staging.stg_reject` (quarantine) beserta rule dan alasannya |
| Reject > 5% / pemeriksaan ERROR gagal | quality gate BLOCK: load dibatalkan, task gagal tanpa retry, alert dikirim |
| Load gagal di tengah | satu transaksi → rollback, staging utuh, aman di-rerun (upsert + unique constraint) |
| DB/server hilang | restore pg_dump dari `orbit-backup`, lalu `orbit_reprocess` membangun ulang dari raw zone |

**Alert.** Semua alert tercatat di `meta.alert` dan tampil di dashboard. Kalau `ALERT_WEBHOOK_URL` diisi, notifikasi
dikirim ke Discord (embed) atau webhook generik (Slack/Teams) untuk task gagal setelah retry habis, quality gate BLOCK,
dan DAG run gagal. Percobaan yang masih akan di-retry hanya dicatat.

Membuat webhook Discord:
1. Buka server Discord → Server Settings → Integrations → Webhooks → New Webhook.
2. Beri nama (mis. *Orbit Pipeline*), pilih channel (mis. `#pipeline-alert`), klik Copy Webhook URL.
3. Tempel di `.env`: `ALERT_WEBHOOK_URL=https://discord.com/api/webhooks/<id>/<token>` lalu
   `docker compose up -d airflow-scheduler airflow-webserver`.
4. URL webhook bersifat rahasia. Simpan hanya di `.env` (sudah di-`.gitignore`).

### Perubahan dari rancangan UTS: dependensi antar hari dilepas

Rancangan UTS (bagian *Scheduling* & *Error Handling* no. 8) menetapkan "jika job sebelumnya gagal, job berikutnya
tidak dijalankan". Di implementasi, aturan ini tetap berlaku di dalam satu run (task hilir hanya jalan kalau task
hulunya sukses, jadi data gagal tidak sampai ke DWH), tapi dilepas antar hari (`depends_on_past=False` pada
`orbit_daily_etl`). Alasannya:

1. Snapshot jadwal tidak bisa diambil ulang. API hanya memberi posisi jadwal saat ini. Dengan aturan UTS, satu
   kegagalan (mis. API timeout hari Senin) menghentikan semua run berikutnya sampai ditangani manual, dan setiap hari
   yang terhenti berarti snapshot `fact_jadwal_snapshot` hari itu hilang (melanggar RPO 1 hari).
2. Keamanan data tetap terjaga lewat mekanisme lain: semua tahap idempotent (checksum, checkpoint, watermark, upsert),
   quality gate menolak data buruk, dan kegagalan dikirim sebagai alert.
3. Pemulihan berjalan sendiri. `transform_ll2_upcoming` memproses semua snapshot di staging yang belum masuk DWH,
   urut dari tanggal paling lama. Snapshot yang ingestion-nya berhasil tapi transform/load-nya gagal akan masuk DWH di
   run berikutnya, dan snapshot sesudahnya dihitung ulang supaya `geser_jam` tetap benar. Snapshot yang diblok quality
   gate tidak menghalangi snapshot lain dan dicoba lagi di run berikutnya.

Diuji oleh `tests/integration_snapshot_recovery.py` (transform tertunda, snapshot diblok, guard sekali per hari)
dan `tests/integration_webhook.py`.

## 4. Data quality, governance & security

- Quality gate per sumber (`meta.quality_gate`) dan sekitar 25 pemeriksaan per transform (`meta.dq_result`) untuk
  completeness, uniqueness, validity/range, consistency, integrity, accuracy, dan timeliness.
- Validasi DWH (`sql/validation/dw_validation.sql`): orphan FK, periode SCD2 tidak tumpang tindih, grain unik,
  rekonsiliasi staging↔fakta, akurasi jumlah peluncuran vs statistik LL2, kesegaran snapshot.
- Lineage: `meta.lineage` (sumber → aturan → tabel tujuan per batch), `meta.lake_object` (manifest + sha256),
  serta kolom `batch_id`/`loaded_at` di setiap fakta.
- Metadata bisnis: `docs/data_catalog.csv` → `meta.data_catalog` (owner, steward, grain, refresh, sensitivitas).
- RBAC: `etl_writer` (pipeline), `analyst_ro` (baca dw/mart/ml/meta), `bi_reader` (dashboard, hanya mart & ml).
  MinIO memakai service account `orbit-pipeline` dengan policy terbatas pada bucket `orbit-*`.
- Credential hanya ada di `.env` (di-ignore Git); `.env.example` berisi placeholder.

## 5. Backup & disaster recovery

| Data | Cara | Frekuensi | Lokasi | Retensi |
|---|---|---|---|---|
| Database `dw_orbit` | `pg_dump -Fc` (client PG16) | harian 12:00 WITA | `orbit-backup/db/YYYY/MM/DD/` + `./backups/db` | 30 hari |
| Raw zone (termasuk snapshot jadwal) | mirror objek baru | harian 12:00 WITA | `orbit-backup/raw/` + `./backups/raw` | permanen |
| Kode, DAG, mapping | Git | tiap perubahan | GitHub | permanen |

Uji restore berjalan otomatis setiap Minggu (`restore_test`): dump diunduh dari object storage, di-`pg_restore` ke
database terpisah, row count 13 tabel dicocokkan, dan RTO aktual dicatat di `meta.restore_test`. RPO = 24 jam.

## 6. Machine learning

Kode ada di `pipeline/ml/`. Ada dua masalah analitik, keduanya membaca `mart.v_peluncuran` (DWH):

1. Risiko kegagalan peluncuran (klasifikasi). Fitur point-in-time: pengalaman roket dan penyedia, riwayat gagal yang
   dihaluskan Bayesian, jeda antar peluncuran, aktivitas industri. Kandidat: baseline (prior, rate penyedia), Logistic
   Regression, Random Forest, HistGradientBoosting. Model dipilih dengan time-series CV 4 fold (2000–2019) dan diuji
   pada hold-out 2020–sekarang. Metrik: ROC-AUC, PR-AUC, Brier, precision/recall/F1, confusion matrix, stabilitas per
   tahun, permutation importance. Output berupa skor risiko tiap jadwal mendatang (`ml.pred_launch_risk`).
2. Forecasting jumlah peluncuran bulanan: naive, seasonal naive, moving average, Ridge dan HistGB berbasis lag.
   Backtest one-step-ahead 24 bulan (MAE, RMSE, MAPE, R²) dan forecast 12 bulan dengan interval 95%.

Selain itu, data drift diukur dengan PSI fitur antar periode dan antar sumber (`ml.drift_report`).

## 7. Hasil implementasi (per 5 November 2026 WITA)

| Area | Hasil |
|---|---|
| Data Warehouse | `fact_peluncuran` 5.837 (4.629 arsip 1957–Jul 2022 + 1.208 LL2 sejak Agu 2022); `fact_jadwal_snapshot` 439 per snapshot harian; `fact_konfigurasi_roket` 532 per minggu; `dim_perusahaan` 111; `dim_lokasi` 215; `dim_roket` 690 versi (SCD2); `dim_tanggal` 30.681 |
| Kualitas data | Validasi DWH 20 cek: 0 ERROR, 1 WARN (ACC-01, dianalisis di [`docs/analisis_acc01.md`](docs/analisis_acc01.md)); reject arsip 1 baris (duplikat identik, TC-01) |
| ML: risiko gagal | Champion Random Forest (dipilih dengan CV PR-AUC 0,352). Test 2020–2026 (1.577 peluncuran): ROC-AUC 0,820, PR-AUC 0,305 vs baseline rate penyedia 0,761 / 0,208 dan baseline prior 0,500 / 0,046 |
| ML: forecasting | Champion moving average 12 bulan (MAE validasi terkecil). Test 24 bulan: MAE 4,38 peluncuran/bulan, MAPE 15,8%. Ridge-lag MAE test 4,31 (setara) |
| Scoring jadwal | 439 jadwal diskor: Tinggi 122, Sedang 120, Rendah 197. Risiko tertinggi ada pada penerbangan perdana oleh penyedia baru (rata-rata p = 0,35) |
| Backup & DR | `pg_dump` 3,6 MB dalam sekitar 1 s, mirror raw 26 objek. Uji restore: 13/13 tabel cocok, RTO 0,9 s |
| Skalabilitas | Data sintetis 1×–50× (5,8 rb → 292 rb baris) dan snapshot 1–3 tahun (480 rb baris): transform sekitar 31 rb baris/s dan konstan (linier), bulk insert staging sekitar 16,8 rb baris/s, query snapshot dengan indeks < 1 ms |
| Dashboard | 6 halaman diuji headless (`streamlit.testing` AppTest): 0 exception |

## 8. Struktur repository

```
orbit-bigdata/
├── dags/                 DAG Airflow (init, daily, weekly, backup, ml, reprocess, perf)
├── pipeline/
│   ├── ingest/           ingestion CSV & LL2 → Data Lake → staging raw
│   ├── transform/        cleaning, validation, standardization, integration, enrichment
│   ├── quality/          DQ framework, quality gate, validasi DWH
│   ├── load/             load dimensi (SCD1/SCD2) & fakta
│   ├── ml/               feature engineering, training, forecast, drift, scoring
│   └── ops/              bootstrap, export lake, backup/DR, reprocess, guard, perf test
├── sql/ddl/              DDL staging, meta, dw, ml, mart, grants
├── sql/validation/       query validasi pasca-load
├── mappings/             tabel pemetaan nama & koreksi manual (versioned)
├── dashboard/            Streamlit app (6 halaman)
├── data/source/          dataset internal asli + kamus data
├── data/sample/          contoh respons JSON LL2 (raw): launches previous/upcoming, launcher_configurations
├── docs/                 katalog data, analisis ACC-01 (veracity antar sumber)
├── tests/                uji integrasi (snapshot recovery, webhook) + smoke test dashboard
├── docker/, docker-compose.yml, requirements.txt, .env.example
```
