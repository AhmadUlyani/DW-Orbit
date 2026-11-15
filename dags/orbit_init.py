"""DAG inisialisasi, dijalankan manual sekali: bootstrap schema + muatan awal penuh.

Muatan awal Launch Library 2 butuh sekitar 24 request (kuota 15/jam). ThrottleGuard menunggu kuota
pulih dan PagedFetcher melanjutkan dari checkpoint terakhir. Semua langkah idempotent, jadi menjalankan
ulang DAG ini tidak menggandakan data.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from orbit_common import DEFAULT_ARGS, START, run, runlog

DAG_ID = "orbit_init"
P = dict(pipeline=DAG_ID)

with DAG(
    dag_id=DAG_ID,
    description="Bootstrap DDL + muatan awal penuh (CSV arsip + LL2 sejak 29 Juli 2022)",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=runlog.on_failure_dag_callback,
    tags=["orbit", "init"],
    doc_md=__doc__,
) as dag:
    T = lambda tid, fn, **kw: PythonOperator(task_id=tid, python_callable=run(fn, **{**P, **kw}),
                                             execution_timeout=timedelta(hours=3))
    ddl = T("apply_ddl", "pipeline.ops.bootstrap.apply_ddl")
    seed = T("seed_catalog_mappings", "pipeline.ops.bootstrap.seed_catalog")
    csv = T("ingest_maven_csv", "pipeline.ingest.maven.ingest_maven")
    stg_csv = T("stage_maven_raw", "pipeline.ingest.maven.stage_maven")
    tr_csv = T("transform_maven", "pipeline.transform.maven.transform_maven")
    up = T("ingest_ll2_upcoming", "pipeline.ingest.ll2.ingest_upcoming", max_wait_s=7400)
    cfg = T("ingest_ll2_configs_full", "pipeline.ingest.ll2.ingest_configs_full", max_wait_s=7400)
    prev = T("ingest_ll2_previous_initial", "pipeline.ingest.ll2.ingest_previous", max_wait_s=7400)
    stg_l = T("stage_ll2_launch_raw", "pipeline.ingest.ll2.stage_launches")
    stg_c = T("stage_ll2_config_raw", "pipeline.ingest.ll2.stage_configs")
    tr_c = T("transform_ll2_configs", "pipeline.transform.ll2.transform_configs")
    tr_l = T("transform_ll2_launches", "pipeline.transform.ll2.transform_launches")
    tr_u = T("transform_ll2_upcoming", "pipeline.transform.ll2.transform_upcoming")
    l_dim = T("load_dimensions", "pipeline.load.warehouse.load_dimensions")
    l_fact = T("load_facts", "pipeline.load.warehouse.load_facts")
    val = T("validate_dw", "pipeline.quality.dw_checks.validate_dw")
    export = T("export_lake_silver_gold", "pipeline.ops.export_lake.export_lake")
    train = TriggerDagRunOperator(task_id="trigger_ml_training", trigger_dag_id="orbit_ml_training")

    ddl >> seed >> [csv, up]
    csv >> stg_csv >> tr_csv
    up >> cfg >> prev >> stg_l >> stg_c >> tr_c
    [tr_csv, tr_c] >> tr_l >> tr_u >> l_dim >> l_fact >> val >> export >> train
