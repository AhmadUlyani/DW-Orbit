-- 02_dw.sql : DATA WAREHOUSE (galaxy schema / fact constellation, Kimball)
-- 6 conformed dimension + 3 fact table

-- DIMENSI
CREATE TABLE IF NOT EXISTS dw.dim_tanggal (
    date_key       INT PRIMARY KEY,                 -- YYYYMMDD
    tanggal        DATE NOT NULL UNIQUE,
    hari           SMALLINT NOT NULL,               -- 1=Senin ... 7=Minggu (ISO)
    nama_hari      VARCHAR(10) NOT NULL,
    minggu_ke      SMALLINT NOT NULL,               -- ISO week
    bulan          SMALLINT NOT NULL,
    nama_bulan     VARCHAR(10) NOT NULL,
    kuartal        SMALLINT NOT NULL,
    tahun          SMALLINT NOT NULL,
    dekade         SMALLINT NOT NULL,
    is_akhir_pekan BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_perusahaan (
    perusahaan_key   SERIAL PRIMARY KEY,            -- surrogate key
    nama_perusahaan  VARCHAR(100) NOT NULL UNIQUE,  -- NK: nama baku
    jenis_perusahaan VARCHAR(30),                   -- Commercial, Government, Multinational, Private
    sumber_data      VARCHAR(20),                   -- Arsip Internal / Launch Library 2 / Keduanya
    updated_at       TIMESTAMPTZ DEFAULT now()      -- SCD Type 1
);

CREATE TABLE IF NOT EXISTS dw.dim_roket (
    roket_key     SERIAL PRIMARY KEY,               -- surrogate key (per versi)
    nama_roket    VARCHAR(100) NOT NULL,            -- NK: nama baku
    ll2_config_id INT,                              -- id konfigurasi LL2, boleh NULL (roket arsip)
    status_roket  VARCHAR(8) NOT NULL CHECK (status_roket IN ('Active','Inactive')),
    start_date    DATE NOT NULL,                    -- SCD Type 2
    end_date      DATE NOT NULL DEFAULT '9999-12-31',
    is_current    BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (nama_roket, start_date)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_roket_current ON dw.dim_roket (nama_roket) WHERE is_current;

CREATE TABLE IF NOT EXISTS dw.dim_lokasi (
    lokasi_key   SERIAL PRIMARY KEY,
    landasan     VARCHAR(100),                      -- NK (bersama situs + negara)
    situs        VARCHAR(100) NOT NULL,
    wilayah      VARCHAR(100),
    kode_negara  CHAR(3),                           -- ISO 3166-1 alpha-3
    latitude     NUMERIC(9,6),
    longitude    NUMERIC(9,6),
    updated_at   TIMESTAMPTZ DEFAULT now()          -- SCD Type 1
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_lokasi_nk
    ON dw.dim_lokasi (COALESCE(landasan,''), situs, COALESCE(kode_negara,'---'));

CREATE TABLE IF NOT EXISTS dw.dim_orbit (
    orbit_key      INT PRIMARY KEY,                 -- -1 = Unknown
    singkatan      VARCHAR(20) NOT NULL UNIQUE,     -- NK: abbrev LL2
    nama_orbit     VARCHAR(60) NOT NULL,
    kelompok_orbit VARCHAR(40) NOT NULL,            -- dari meta.map_orbit
    updated_at     TIMESTAMPTZ DEFAULT now()
);
CREATE SEQUENCE IF NOT EXISTS dw.seq_orbit_key START 1;

CREATE TABLE IF NOT EXISTS dw.dim_status (
    status_key      SERIAL PRIMARY KEY,
    nama_status     VARCHAR(40) NOT NULL UNIQUE,    -- NK
    kelompok_status VARCHAR(20) NOT NULL CHECK (kelompok_status IN ('Hasil','Jadwal')),
    is_final        BOOLEAN NOT NULL
);

-- FAKTA
-- Grain: 1 baris = 1 peluncuran (transaction fact)
CREATE TABLE IF NOT EXISTS dw.fact_peluncuran (
    peluncuran_key   BIGSERIAL PRIMARY KEY,
    sumber_id        VARCHAR(64) NOT NULL UNIQUE,   -- NK: MVN-hash atau UUID LL2
    date_key         INT NOT NULL REFERENCES dw.dim_tanggal(date_key),
    perusahaan_key   INT NOT NULL REFERENCES dw.dim_perusahaan(perusahaan_key),
    roket_key        INT NOT NULL REFERENCES dw.dim_roket(roket_key),
    lokasi_key       INT NOT NULL REFERENCES dw.dim_lokasi(lokasi_key),
    status_key       INT NOT NULL REFERENCES dw.dim_status(status_key),
    orbit_key        INT NOT NULL REFERENCES dw.dim_orbit(orbit_key),
    waktu_peluncuran TIME,                          -- UTC, boleh NULL
    waktu_diketahui  SMALLINT NOT NULL,
    nama_misi        VARCHAR(200),                  -- degenerate dimension
    jenis_misi       VARCHAR(60),                   -- degenerate dimension
    alasan_gagal     TEXT,                          -- degenerate dimension
    harga_musd       NUMERIC(10,2),                 -- boleh NULL
    sumber_harga     VARCHAR(20),
    is_sukses        SMALLINT NOT NULL CHECK (is_sukses IN (0,1)),
    sumber_data      VARCHAR(20) NOT NULL,
    batch_id         BIGINT,
    loaded_at        TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fp_date       ON dw.fact_peluncuran (date_key);
CREATE INDEX IF NOT EXISTS ix_fp_perusahaan ON dw.fact_peluncuran (perusahaan_key);
CREATE INDEX IF NOT EXISTS ix_fp_roket      ON dw.fact_peluncuran (roket_key);
CREATE INDEX IF NOT EXISTS ix_fp_lokasi     ON dw.fact_peluncuran (lokasi_key);
CREATE INDEX IF NOT EXISTS ix_fp_status     ON dw.fact_peluncuran (status_key);
CREATE INDEX IF NOT EXISTS ix_fp_orbit      ON dw.fact_peluncuran (orbit_key);

-- Grain: 1 baris = 1 konfigurasi roket per minggu (periodic snapshot)
CREATE TABLE IF NOT EXISTS dw.fact_konfigurasi_roket (
    konfigurasi_key      BIGSERIAL PRIMARY KEY,
    snapshot_date_key    INT NOT NULL REFERENCES dw.dim_tanggal(date_key),
    roket_key            INT NOT NULL REFERENCES dw.dim_roket(roket_key),
    produsen_key         INT REFERENCES dw.dim_perusahaan(perusahaan_key),
    ll2_config_id        INT NOT NULL,
    perkiraan_biaya_musd NUMERIC(10,2),             -- boleh NULL
    total_peluncuran     INT,
    peluncuran_sukses    INT,
    peluncuran_gagal     INT,
    sukses_beruntun      INT,
    peluncuran_terjadwal INT,
    batch_id             BIGINT,
    loaded_at            TIMESTAMPTZ DEFAULT now(),
    UNIQUE (ll2_config_id, snapshot_date_key)
);
CREATE INDEX IF NOT EXISTS ix_fkr_snapshot ON dw.fact_konfigurasi_roket (snapshot_date_key);
CREATE INDEX IF NOT EXISTS ix_fkr_roket    ON dw.fact_konfigurasi_roket (roket_key);
CREATE INDEX IF NOT EXISTS ix_fkr_produsen ON dw.fact_konfigurasi_roket (produsen_key);

-- Grain: 1 baris = 1 jadwal peluncuran per tanggal snapshot (periodic snapshot harian)
CREATE TABLE IF NOT EXISTS dw.fact_jadwal_snapshot (
    jadwal_key             BIGSERIAL PRIMARY KEY,
    snapshot_date_key      INT NOT NULL REFERENCES dw.dim_tanggal(date_key),  -- role-playing
    net_date_key           INT NOT NULL REFERENCES dw.dim_tanggal(date_key),  -- role-playing
    ll2_launch_id          UUID NOT NULL,
    perusahaan_key         INT NOT NULL REFERENCES dw.dim_perusahaan(perusahaan_key),
    roket_key              INT NOT NULL REFERENCES dw.dim_roket(roket_key),
    lokasi_key             INT NOT NULL REFERENCES dw.dim_lokasi(lokasi_key),
    status_key             INT NOT NULL REFERENCES dw.dim_status(status_key),
    orbit_key              INT NOT NULL REFERENCES dw.dim_orbit(orbit_key),
    net_waktu              TIMESTAMPTZ NOT NULL,
    window_start           TIMESTAMPTZ,
    window_end             TIMESTAMPTZ,
    presisi_jadwal         VARCHAR(30),
    nama_misi              VARCHAR(200),
    jenis_misi             VARCHAR(60),
    hari_menuju_peluncuran INT,
    geser_jam              NUMERIC(10,2),           -- NULL pada snapshot pertama
    is_bergeser            SMALLINT,
    batch_id               BIGINT,
    loaded_at              TIMESTAMPTZ DEFAULT now(),
    UNIQUE (ll2_launch_id, snapshot_date_key)
);
CREATE INDEX IF NOT EXISTS ix_fjs_snapshot   ON dw.fact_jadwal_snapshot (snapshot_date_key);
CREATE INDEX IF NOT EXISTS ix_fjs_net        ON dw.fact_jadwal_snapshot (net_date_key);
CREATE INDEX IF NOT EXISTS ix_fjs_perusahaan ON dw.fact_jadwal_snapshot (perusahaan_key);
CREATE INDEX IF NOT EXISTS ix_fjs_roket      ON dw.fact_jadwal_snapshot (roket_key);
CREATE INDEX IF NOT EXISTS ix_fjs_lokasi     ON dw.fact_jadwal_snapshot (lokasi_key);
CREATE INDEX IF NOT EXISTS ix_fjs_status     ON dw.fact_jadwal_snapshot (status_key);
CREATE INDEX IF NOT EXISTS ix_fjs_orbit      ON dw.fact_jadwal_snapshot (orbit_key);

-- SEED dimensi statis
-- dim_tanggal 1957-01-01 s.d. 2040-12-31 (30.681 baris)
INSERT INTO dw.dim_tanggal
SELECT to_char(d,'YYYYMMDD')::int, d::date,
       EXTRACT(ISODOW FROM d)::smallint,
       (ARRAY['Senin','Selasa','Rabu','Kamis','Jumat','Sabtu','Minggu'])[EXTRACT(ISODOW FROM d)],
       EXTRACT(WEEK FROM d)::smallint,
       EXTRACT(MONTH FROM d)::smallint,
       (ARRAY['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus',
              'September','Oktober','November','Desember'])[EXTRACT(MONTH FROM d)],
       EXTRACT(QUARTER FROM d)::smallint,
       EXTRACT(YEAR FROM d)::smallint,
       (EXTRACT(YEAR FROM d)::int / 10 * 10)::smallint,
       EXTRACT(ISODOW FROM d) IN (6,7)
FROM generate_series('1957-01-01'::date, '2040-12-31'::date, interval '1 day') d
ON CONFLICT (date_key) DO NOTHING;

-- dim_status: kelompok Hasil (4 kategori arsip) + Jadwal (status LL2 belum final)
INSERT INTO dw.dim_status (nama_status, kelompok_status, is_final) VALUES
 ('Success','Hasil',TRUE), ('Failure','Hasil',TRUE),
 ('Partial Failure','Hasil',TRUE), ('Prelaunch Failure','Hasil',TRUE),
 ('Go for Launch','Jadwal',FALSE), ('To Be Determined','Jadwal',FALSE),
 ('To Be Confirmed','Jadwal',FALSE), ('On Hold','Jadwal',FALSE),
 ('Launch in Flight','Jadwal',FALSE), ('Payload Deployed','Jadwal',FALSE)
ON CONFLICT (nama_status) DO NOTHING;

-- dim_orbit: baris Unknown (orbit_key = -1) agar FK tidak NULL
INSERT INTO dw.dim_orbit (orbit_key, singkatan, nama_orbit, kelompok_orbit)
VALUES (-1, 'N/A', 'Unknown', 'Tidak Diketahui')
ON CONFLICT (orbit_key) DO NOTHING;
