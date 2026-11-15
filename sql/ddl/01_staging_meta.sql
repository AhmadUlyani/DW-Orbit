-- 01_staging_meta.sql : layer STAGING (raw + clean + quarantine) dan META
-- Idempotent: aman dijalankan ulang (IF NOT EXISTS).
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS perf;

-- META: operational metadata (log ETL, request API, watermark, checkpoint)
CREATE TABLE IF NOT EXISTS meta.etl_batch (
    batch_id       BIGSERIAL PRIMARY KEY,
    pipeline       VARCHAR(60)  NOT NULL,          -- nama DAG / job
    task           VARCHAR(80)  NOT NULL,          -- nama task
    run_id         VARCHAR(250),                   -- dag_run_id Airflow
    source         VARCHAR(60),
    status         VARCHAR(12)  NOT NULL DEFAULT 'RUNNING'
                   CHECK (status IN ('RUNNING','SUCCESS','FAILED','SKIPPED','PARTIAL','BLOCKED')),
    started_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    duration_s     NUMERIC(12,3),
    rows_read      BIGINT DEFAULT 0,
    rows_written   BIGINT DEFAULT 0,
    rows_rejected  BIGINT DEFAULT 0,
    api_requests   INT    DEFAULT 0,
    attempt        INT    DEFAULT 1,
    message        TEXT,
    error_trace    TEXT
);
CREATE INDEX IF NOT EXISTS ix_etl_batch_started ON meta.etl_batch (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_etl_batch_task    ON meta.etl_batch (pipeline, task, status);

CREATE TABLE IF NOT EXISTS meta.api_request_log (
    request_id   BIGSERIAL PRIMARY KEY,
    batch_id     BIGINT REFERENCES meta.etl_batch(batch_id),
    endpoint     VARCHAR(80),
    url          TEXT,
    http_status  INT,
    attempt      INT,
    duration_ms  INT,
    bytes        BIGINT,
    quota_used   INT,
    error        TEXT,
    requested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.watermark (
    name       VARCHAR(80) PRIMARY KEY,               -- mis. ll2_previous_last_updated
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- checkpoint paging API agar ingestion dapat dilanjutkan (resumable) setelah kuota habis/gagal
CREATE TABLE IF NOT EXISTS meta.ingest_checkpoint (
    job          VARCHAR(80) PRIMARY KEY,             -- mis. ll2_upcoming@2026-11-04
    next_url     TEXT,
    pages_done   INT DEFAULT 0,
    records_done INT DEFAULT 0,
    total_count  INT,
    status       VARCHAR(12) DEFAULT 'IN_PROGRESS' CHECK (status IN ('IN_PROGRESS','DONE')),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

-- katalog objek Data Lake (manifest) -> lineage & audit
CREATE TABLE IF NOT EXISTS meta.lake_object (
    object_key    TEXT PRIMARY KEY,
    bucket        VARCHAR(40) NOT NULL,
    zone          VARCHAR(10) NOT NULL CHECK (zone IN ('raw','clean','curated','backup')),
    source        VARCHAR(40),
    dataset       VARCHAR(60),
    batch_id      BIGINT,
    ingest_date   DATE,
    record_count  INT,
    size_bytes    BIGINT,
    sha256        CHAR(64),
    content_type  VARCHAR(60),
    created_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_lake_object_ds ON meta.lake_object (zone, dataset, ingest_date);

-- META: Data Quality & Quality Gate
CREATE TABLE IF NOT EXISTS meta.dq_result (
    dq_id         BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT,
    layer         VARCHAR(10),        -- staging / dw
    table_name    VARCHAR(80),
    rule_id       VARCHAR(10),        -- TV-xx / TC-xx / DQ-xx
    check_name    VARCHAR(120),
    dimension     VARCHAR(20),        -- completeness, uniqueness, validity, consistency, integrity, accuracy, timeliness
    severity      VARCHAR(5) CHECK (severity IN ('ERROR','WARN','INFO')),
    status        VARCHAR(4) CHECK (status IN ('PASS','FAIL')),
    total_rows    BIGINT,
    failed_rows   BIGINT,
    fail_pct      NUMERIC(7,3),
    threshold_pct NUMERIC(7,3),
    details       TEXT,
    checked_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_result_batch ON meta.dq_result (batch_id);

CREATE TABLE IF NOT EXISTS meta.quality_gate (
    gate_id       BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT,
    source        VARCHAR(40),
    total_rows    BIGINT,
    rejected_rows BIGINT,
    reject_pct    NUMERIC(7,3),
    threshold_pct NUMERIC(7,3),
    error_checks_failed INT,
    decision      VARCHAR(5) CHECK (decision IN ('PASS','BLOCK')),
    decided_at    TIMESTAMPTZ DEFAULT now()
);

-- META: lineage, katalog (business metadata), alert, review queue
CREATE TABLE IF NOT EXISTS meta.lineage (
    lineage_id     BIGSERIAL PRIMARY KEY,
    batch_id       BIGINT,
    target_table   VARCHAR(80),
    target_layer   VARCHAR(10),
    source_refs    TEXT[],             -- objek lake / tabel sumber
    rules_applied  TEXT[],             -- ID aturan transformasi
    rows_affected  BIGINT,
    created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.data_catalog (
    table_name   VARCHAR(80) PRIMARY KEY,
    layer        VARCHAR(10),
    description  TEXT,
    grain        TEXT,
    owner        VARCHAR(80),          -- data owner
    steward      VARCHAR(80),          -- data steward
    refresh      VARCHAR(60),
    sensitivity  VARCHAR(20) DEFAULT 'Internal',
    consumers    TEXT
);

CREATE TABLE IF NOT EXISTS meta.alert (
    alert_id     BIGSERIAL PRIMARY KEY,
    level        VARCHAR(8) CHECK (level IN ('INFO','WARNING','ERROR','CRITICAL')),
    dag_id       VARCHAR(80),
    task_id      VARCHAR(80),
    run_id       VARCHAR(250),
    message      TEXT,
    created_at   TIMESTAMPTZ DEFAULT now(),
    acknowledged BOOLEAN DEFAULT FALSE
);

-- TI-05: nama baru yang belum terpetakan -> ditinjau data steward
CREATE TABLE IF NOT EXISTS meta.review_queue (
    review_id    BIGSERIAL PRIMARY KEY,
    entity_type  VARCHAR(20),          -- perusahaan / roket / lokasi / orbit / status
    raw_value    TEXT,
    sample_ref   TEXT,
    first_batch  BIGINT,
    status       VARCHAR(10) DEFAULT 'PENDING' CHECK (status IN ('PENDING','MAPPED','IGNORED')),
    created_at   TIMESTAMPTZ DEFAULT now(),
    UNIQUE (entity_type, raw_value)
);

-- META: tabel pemetaan (seed dari folder mappings/, versioned di Git)
CREATE TABLE IF NOT EXISTS meta.map_perusahaan (
    nama_sumber      VARCHAR(150) PRIMARY KEY,   -- nama dari LL2 (launch_service_provider / manufacturer)
    nama_baku        VARCHAR(100) NOT NULL,       -- nama baku (mengikuti arsip internal)
    jenis_perusahaan VARCHAR(30)
);
CREATE TABLE IF NOT EXISTS meta.map_roket (
    nama_sumber  VARCHAR(150) PRIMARY KEY,        -- full_name LL2
    nama_baku    VARCHAR(100) NOT NULL
);
CREATE TABLE IF NOT EXISTS meta.map_lokasi (
    pad_sumber   VARCHAR(150),
    situs_sumber VARCHAR(150),
    landasan_baku VARCHAR(100),
    situs_baku    VARCHAR(100),
    PRIMARY KEY (pad_sumber, situs_sumber)
);
CREATE TABLE IF NOT EXISTS meta.map_orbit (
    singkatan       VARCHAR(20) PRIMARY KEY,
    kelompok_orbit  VARCHAR(40) NOT NULL
);
CREATE TABLE IF NOT EXISTS meta.map_negara (
    nama_sumber  VARCHAR(80) PRIMARY KEY,         -- bagian terakhir lokasi arsip
    kode_negara  CHAR(3),                          -- ISO 3166-1 alpha-3, NULL = perairan internasional
    wilayah_tambahan VARCHAR(80)
);
CREATE TABLE IF NOT EXISTS meta.koreksi_manual (
    kolom          VARCHAR(40),
    nilai_asli     TEXT,
    nilai_koreksi  TEXT NOT NULL,
    alasan         TEXT,
    PRIMARY KEY (kolom, nilai_asli)
);

-- META: disaster recovery & performance test
CREATE TABLE IF NOT EXISTS meta.backup_log (
    backup_id      BIGSERIAL PRIMARY KEY,
    backup_type    VARCHAR(20),      -- db_full / raw_mirror / model
    object_key     TEXT,
    local_path     TEXT,
    size_bytes     BIGINT,
    sha256         CHAR(64),
    objects_copied INT,
    status         VARCHAR(10),
    started_at     TIMESTAMPTZ DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    retention_until DATE,
    message        TEXT
);
CREATE TABLE IF NOT EXISTS meta.restore_test (
    test_id      BIGSERIAL PRIMARY KEY,
    backup_key   TEXT,
    target_db    VARCHAR(60),
    tables_checked INT,
    tables_matched INT,
    details      JSONB,
    duration_s   NUMERIC(10,3),
    status       VARCHAR(10),
    tested_at    TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS meta.perf_result (
    perf_id       BIGSERIAL PRIMARY KEY,
    run_label     VARCHAR(60),
    test_name     VARCHAR(80),
    stage         VARCHAR(40),
    scale_factor  NUMERIC(8,2),
    rows          BIGINT,
    duration_s    NUMERIC(12,4),
    throughput_rps NUMERIC(14,2),
    peak_mem_mb   NUMERIC(10,2),
    notes         TEXT,
    run_at        TIMESTAMPTZ DEFAULT now()
);

-- STAGING: RAW (sedekat mungkin dengan sumber) -- hanya teks / jsonb
CREATE TABLE IF NOT EXISTS staging.stg_maven_raw (
    batch_id        BIGINT NOT NULL,
    row_num         INT    NOT NULL,          -- nomor baris di file sumber
    company         TEXT, location TEXT, date TEXT, time TEXT, rocket TEXT,
    mission         TEXT, rocket_status TEXT, price TEXT, mission_status TEXT,
    source_object   TEXT,
    source_sha256   CHAR(64),
    extracted_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (batch_id, row_num)
);

CREATE TABLE IF NOT EXISTS staging.stg_ll2_launch_raw (
    raw_id          BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT NOT NULL,
    endpoint        VARCHAR(20) NOT NULL CHECK (endpoint IN ('previous','upcoming')),
    ll2_launch_id   UUID NOT NULL,
    last_updated    TIMESTAMPTZ,
    snapshot_date   DATE NOT NULL,
    payload         JSONB NOT NULL,
    source_object   TEXT,
    extracted_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_ll2_launch_raw ON staging.stg_ll2_launch_raw (endpoint, batch_id);
CREATE INDEX IF NOT EXISTS ix_stg_ll2_launch_raw_id ON staging.stg_ll2_launch_raw (ll2_launch_id, last_updated DESC);

CREATE TABLE IF NOT EXISTS staging.stg_ll2_config_raw (
    raw_id          BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT NOT NULL,
    ll2_config_id   INT NOT NULL,
    snapshot_date   DATE NOT NULL,
    payload         JSONB NOT NULL,
    source_object   TEXT,
    extracted_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_ll2_config_raw ON staging.stg_ll2_config_raw (batch_id, ll2_config_id);

-- STAGING: CLEAN (silver) -- hasil cleaning, validation, standardization
CREATE TABLE IF NOT EXISTS staging.stg_peluncuran_clean (
    sumber_id         VARCHAR(64) PRIMARY KEY,     -- MVN-<hash> atau UUID LL2
    sumber_data       VARCHAR(20) NOT NULL,        -- Arsip Internal / Launch Library 2
    tanggal           DATE NOT NULL,
    waktu             TIME,
    waktu_diketahui   SMALLINT NOT NULL,
    perusahaan_nama   VARCHAR(100) NOT NULL,
    perusahaan_jenis  VARCHAR(30),
    roket_nama        VARCHAR(100) NOT NULL,
    ll2_config_id     INT,
    roket_status      VARCHAR(8),
    landasan          VARCHAR(100),
    situs             VARCHAR(100),
    wilayah           VARCHAR(100),
    kode_negara       CHAR(3),
    latitude          NUMERIC(9,6),
    longitude         NUMERIC(9,6),
    orbit_singkatan   VARCHAR(20),
    orbit_nama        VARCHAR(60),
    status_nama       VARCHAR(40) NOT NULL,
    nama_misi         VARCHAR(200),
    jenis_misi        VARCHAR(60),
    alasan_gagal      TEXT,
    harga_musd        NUMERIC(10,2),
    sumber_harga      VARCHAR(20),
    is_sukses         SMALLINT NOT NULL,
    last_updated      TIMESTAMPTZ,
    batch_id          BIGINT,
    transformed_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.stg_jadwal_clean (
    ll2_launch_id     UUID NOT NULL,
    snapshot_date     DATE NOT NULL,
    net_waktu         TIMESTAMPTZ NOT NULL,
    window_start      TIMESTAMPTZ,
    window_end        TIMESTAMPTZ,
    presisi_jadwal    VARCHAR(30),
    status_nama       VARCHAR(40) NOT NULL,
    perusahaan_nama   VARCHAR(100) NOT NULL,
    perusahaan_jenis  VARCHAR(30),
    roket_nama        VARCHAR(100) NOT NULL,
    ll2_config_id     INT,
    landasan          VARCHAR(100),
    situs             VARCHAR(100),
    wilayah           VARCHAR(100),
    kode_negara       CHAR(3),
    latitude          NUMERIC(9,6),
    longitude         NUMERIC(9,6),
    orbit_singkatan   VARCHAR(20),
    orbit_nama        VARCHAR(60),
    nama_misi         VARCHAR(200),
    jenis_misi        VARCHAR(60),
    hari_menuju_peluncuran INT,
    geser_jam         NUMERIC(10,2),
    is_bergeser       SMALLINT,
    batch_id          BIGINT,
    transformed_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ll2_launch_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS staging.stg_roket_clean (
    ll2_config_id     INT NOT NULL,
    snapshot_date     DATE NOT NULL,
    roket_nama        VARCHAR(100) NOT NULL,
    produsen_nama     VARCHAR(100),
    produsen_jenis    VARCHAR(30),
    status_roket      VARCHAR(8),
    perkiraan_biaya_musd NUMERIC(10,2),
    total_peluncuran  INT,
    peluncuran_sukses INT,
    peluncuran_gagal  INT,
    sukses_beruntun   INT,
    peluncuran_terjadwal INT,
    is_full_snapshot  BOOLEAN NOT NULL DEFAULT TRUE,   -- FALSE = diambil per-id (harian), bukan snapshot mingguan
    batch_id          BIGINT,
    transformed_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ll2_config_id, snapshot_date)
);

-- QUARANTINE: baris yang gagal validasi tidak dibuang, disimpan beserta alasannya
CREATE TABLE IF NOT EXISTS staging.stg_reject (
    reject_id     BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT,
    source        VARCHAR(40),
    target_table  VARCHAR(60),
    record_ref    TEXT,
    rule_id       VARCHAR(10),
    reason        TEXT,
    payload       JSONB,
    rejected_at   TIMESTAMPTZ DEFAULT now(),
    resolved      BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS ix_stg_reject_batch ON staging.stg_reject (batch_id, rule_id);
