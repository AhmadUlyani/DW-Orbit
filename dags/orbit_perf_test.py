"""DAG uji skalabilitas & performa (manual).

Mengukur throughput transform, tulis Data Lake, bulk insert staging, load fakta, dan latensi query
OLAP pada skala 1x-50x memakai data sintetis berlabel di schema `perf` (tidak menyentuh dw/mart).
Hasil disimpan di meta.perf_result.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from orbit_common import DEFAULT_ARGS, START, run, runlog

DAG_ID = "orbit_perf_test"

with DAG(
    dag_id=DAG_ID,
    description="Uji skalabilitas pipeline & query (data sintetis berlabel, schema perf)",
    schedule=None,
    start_date=START,
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0, "execution_timeout": timedelta(hours=1)},
    tags=["orbit", "performance"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="scalability_test", python_callable=run("pipeline.ops.perf_test.run", pipeline=DAG_ID))
