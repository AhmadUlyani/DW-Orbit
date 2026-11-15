"""DAG reprocessing (manual): bangun ulang staging + DWH dari Data Lake raw zone.

Tidak memanggil API. Dipakai setelah aturan transformasi diperbaiki, atau setelah database
dipulihkan dari backup yang lebih lama dari isi Data Lake.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator

from orbit_common import DEFAULT_ARGS, START, run, runlog

DAG_ID = "orbit_reprocess"
P = dict(pipeline=DAG_ID)

with DAG(
    dag_id=DAG_ID,
    description="Rebuild staging + DWH dari Data Lake (tanpa API)",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    on_failure_callback=runlog.on_failure_dag_callback,
    tags=["orbit", "reprocess", "dr"],
    doc_md=__doc__,
) as dag:
    T = lambda tid, fn: PythonOperator(task_id=tid, python_callable=run(fn, **P))
    (T("reset_staging_dw", "pipeline.ops.reprocess.reset_for_reprocess")
     >> T("seed_catalog_mappings", "pipeline.ops.bootstrap.seed_catalog")
     >> T("stage_maven_raw", "pipeline.ingest.maven.stage_maven")
     >> T("stage_ll2_launch_raw", "pipeline.ingest.ll2.stage_launches")
     >> T("stage_ll2_config_raw", "pipeline.ingest.ll2.stage_configs")
     >> T("transform_maven", "pipeline.transform.maven.transform_maven")
     >> T("transform_ll2_configs", "pipeline.transform.ll2.transform_configs")
     >> T("transform_ll2_launches", "pipeline.transform.ll2.transform_launches")
     >> T("transform_all_snapshots", "pipeline.ops.reprocess.transform_all_snapshots")
     >> T("load_dimensions", "pipeline.load.warehouse.load_dimensions")
     >> T("load_facts", "pipeline.load.warehouse.load_facts")
     >> T("validate_dw", "pipeline.quality.dw_checks.validate_dw")
     >> T("export_lake_silver_gold", "pipeline.ops.export_lake.export_lake"))
