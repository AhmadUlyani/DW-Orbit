-- 03_ml.sql : ANALYTICAL LAYER untuk Machine Learning
CREATE TABLE IF NOT EXISTS ml.model_registry (
    model_id      BIGSERIAL PRIMARY KEY,
    model_name    VARCHAR(60) NOT NULL,         -- launch_risk / launch_forecast
    version       VARCHAR(40) NOT NULL,         -- timestamp training
    candidate     VARCHAR(60) NOT NULL,         -- baseline / logreg / hgb ...
    algorithm     VARCHAR(80),
    params        JSONB,
    features      TEXT[],
    train_period  VARCHAR(40),
    valid_period  VARCHAR(40),
    test_period   VARCHAR(40),
    n_train       INT,
    n_test        INT,
    metrics       JSONB,                        -- metrik test set
    artifact_key  TEXT,                         -- lokasi model di Data Lake (orbit-curated/models/..)
    is_champion   BOOLEAN DEFAULT FALSE,
    trained_at    TIMESTAMPTZ DEFAULT now(),
    UNIQUE (model_name, version, candidate)
);

-- seluruh hasil eksperimen (baseline -> improvement -> comparison)
CREATE TABLE IF NOT EXISTS ml.model_experiment (
    exp_id       BIGSERIAL PRIMARY KEY,
    model_name   VARCHAR(60),
    version      VARCHAR(40),
    candidate    VARCHAR(60),
    split        VARCHAR(20),                   -- cv / valid / test / test_yyyy
    metric       VARCHAR(30),
    value        NUMERIC(14,6),
    created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ml.feature_importance (
    model_name  VARCHAR(60),
    version     VARCHAR(40),
    candidate   VARCHAR(60),
    feature     VARCHAR(80),
    importance  NUMERIC(14,6),
    importance_std NUMERIC(14,6),
    method      VARCHAR(40),
    PRIMARY KEY (model_name, version, candidate, feature)
);

-- prediksi risiko kegagalan pada data uji (actual vs prediction)
CREATE TABLE IF NOT EXISTS ml.pred_launch_backtest (
    model_name  VARCHAR(60),
    version     VARCHAR(40),
    candidate   VARCHAR(60),
    sumber_id   VARCHAR(64),
    tanggal     DATE,
    actual_gagal SMALLINT,
    p_gagal     NUMERIC(8,6),
    pred_gagal  SMALLINT,
    PRIMARY KEY (model_name, version, candidate, sumber_id)
);

-- skor risiko untuk jadwal peluncuran mendatang (output model yang dipakai bisnis)
CREATE TABLE IF NOT EXISTS ml.pred_launch_risk (
    ll2_launch_id  UUID,
    snapshot_date  DATE,
    model_version  VARCHAR(40),
    p_gagal        NUMERIC(8,6),
    risk_band      VARCHAR(10),                 -- Rendah / Sedang / Tinggi
    faktor_utama   TEXT,
    scored_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ll2_launch_id, snapshot_date)
);

-- forecasting jumlah peluncuran bulanan
CREATE TABLE IF NOT EXISTS ml.forecast_monthly (
    model_name  VARCHAR(60),
    version     VARCHAR(40),
    candidate   VARCHAR(60),
    bulan       DATE,
    segment     VARCHAR(60) DEFAULT 'Global',
    actual      NUMERIC(10,2),
    predicted   NUMERIC(10,2),
    lower_pi    NUMERIC(10,2),
    upper_pi    NUMERIC(10,2),
    is_future   BOOLEAN,
    PRIMARY KEY (model_name, version, candidate, bulan, segment)
);

-- data drift (PSI) & stabilitas model per periode
CREATE TABLE IF NOT EXISTS ml.drift_report (
    drift_id     BIGSERIAL PRIMARY KEY,
    version      VARCHAR(40),
    feature      VARCHAR(80),
    ref_period   VARCHAR(30),
    cur_period   VARCHAR(30),
    psi          NUMERIC(10,5),
    ref_mean     NUMERIC(14,5),
    cur_mean     NUMERIC(14,5),
    status       VARCHAR(12),                   -- Stabil / Moderat / Signifikan
    created_at   TIMESTAMPTZ DEFAULT now()
);
