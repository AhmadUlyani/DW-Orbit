-- 05_grants.sql : Governance & Security (RBAC least privilege)
--   etl_writer : owner seluruh objek (dipakai Airflow)
--   analyst_ro : SELECT dw, mart, ml, meta (data analyst)
--   bi_reader  : SELECT mart & ml saja (dashboard). Tidak bisa membaca staging/dw/meta
--                secara langsung; data monitoring diakses lewat view mart.v_*.
-- Variabel psql: :bi_user, :analyst_user
GRANT USAGE ON SCHEMA mart, ml TO :bi_user;
GRANT SELECT ON ALL TABLES IN SCHEMA mart, ml TO :bi_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO :bi_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml   GRANT SELECT ON TABLES TO :bi_user;

GRANT USAGE ON SCHEMA dw, mart, ml, meta TO :analyst_user;
GRANT SELECT ON ALL TABLES IN SCHEMA dw, mart, ml, meta TO :analyst_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw   GRANT SELECT ON TABLES TO :analyst_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO :analyst_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml   GRANT SELECT ON TABLES TO :analyst_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA meta GRANT SELECT ON TABLES TO :analyst_user;

REVOKE ALL ON SCHEMA staging, meta, dw, perf FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
