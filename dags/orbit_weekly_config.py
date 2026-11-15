"""DAG mingguan (Minggu 13:30 WITA): snapshot penuh konfigurasi roket LL2 (sekitar 6 request).

Memperbarui status aktif roket (SCD2 di dim_roket) dan menambah satu snapshot fact_konfigurasi_roket.
Jamnya digeser satu jam dari ETL harian supaya kuota 15 request/jam tidak dipakai bersamaan.

guard_once_per_week men-skip run terjadwal kalau snapshot minggu ISO ini sudah ada, supaya grain
fact_konfigurasi_roket tetap satu snapshot per minggu. Trigger manual selalu jalan.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from orbit_common import DEFAULT_ARGS, START, run, runlog

DAG_ID = "orbit_weekly_config"
P = dict(pipeline=DAG_ID)


def _guard_week(**context):
    from pipeline.ops.guard import no_config_snapshot_this_week   # import lazy, lihat orbit_common
    return no_config_snapshot_this_week(DAG_ID, force=bool(context["dag_run"].external_trigger))


with DAG(
    dag_id=DAG_ID,
    description="Snapshot mingguan konfigurasi roket -> dim_roket (SCD2) & fact_konfigurasi_roket",
    schedule="30 13 * * 0",           # Minggu 13:30 WITA
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=runlog.on_failure_dag_callback,
    tags=["orbit", "etl", "weekly"],
    doc_md=__doc__,
) as dag:
    T = lambda tid, fn, **kw: PythonOperator(task_id=tid, python_callable=run(fn, **{**P, **kw}))
    ingest = PythonOperator(task_id="ingest_ll2_configs_full",
                            python_callable=run("pipeline.ingest.ll2.ingest_configs_full", **P),
                            execution_timeout=timedelta(hours=2))
    guard = ShortCircuitOperator(task_id="guard_once_per_week", python_callable=_guard_week,
                                 ignore_downstream_trigger_rules=True, retries=1)
    (guard
     >> ingest
     >> T("stage_ll2_config_raw", "pipeline.ingest.ll2.stage_configs")
     >> T("transform_ll2_configs", "pipeline.transform.ll2.transform_configs")
     >> T("load_dimensions", "pipeline.load.warehouse.load_dimensions")
     >> T("load_facts", "pipeline.load.warehouse.load_facts")
     >> PythonOperator(task_id="validate_dw", python_callable=run("pipeline.quality.dw_checks.validate_dw", **P),
                       retries=0))
