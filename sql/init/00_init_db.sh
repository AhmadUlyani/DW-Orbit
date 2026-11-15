#!/bin/bash
# Dijalankan sekali oleh image postgres saat volume data masih kosong.
# 1) membuat database Airflow & Data Warehouse, 2) role RBAC, 3) DDL semua layer.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username postgres <<SQL
CREATE ROLE airflow LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
CREATE DATABASE airflow OWNER airflow;
-- RBAC Data Warehouse (least privilege)
CREATE ROLE ${DW_ETL_USER}     LOGIN CREATEDB PASSWORD '${DW_ETL_PASSWORD}';  -- pipeline ETL (owner objek; CREATEDB untuk uji restore DR)
CREATE ROLE ${DW_ANALYST_USER} LOGIN PASSWORD '${DW_ANALYST_PASSWORD}'; -- data analyst: read dw/mart/ml/meta
CREATE ROLE ${DW_BI_USER}      LOGIN PASSWORD '${DW_BI_PASSWORD}';      -- dashboard: read mart & ml saja
CREATE DATABASE dw_orbit OWNER ${DW_ETL_USER};
REVOKE ALL ON DATABASE dw_orbit FROM PUBLIC;
GRANT CONNECT ON DATABASE dw_orbit TO ${DW_ANALYST_USER}, ${DW_BI_USER};
SQL
# DDL dijalankan sebagai owner (etl_writer) agar seluruh objek dimiliki role pipeline
for f in /opt/ddl/*.sql; do
  echo "[init] apply $f"
  psql -v ON_ERROR_STOP=1 --username "${DW_ETL_USER}" -d dw_orbit \
       -v bi_user="${DW_BI_USER}" -v analyst_user="${DW_ANALYST_USER}" -f "$f"
done
