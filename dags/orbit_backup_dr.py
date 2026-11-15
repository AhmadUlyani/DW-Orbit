r"""DAG backup & disaster recovery, jalan 12:00 WITA sebelum ETL harian.

backup_database (pg_dump)  ->  mirror_raw_zone  ->  retention_cleanup
                                               \->  [Minggu] restore_test (ukur RTO)
RPO 24 jam (backup harian), target RTO <= 4 jam. RTO aktual dicatat di meta.restore_test.
"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from orbit_common import DEFAULT_ARGS, START, TZ, run, runlog

DAG_ID = "orbit_backup_dr"
P = dict(pipeline=DAG_ID)


def _is_sunday(**context):
    d = context["data_interval_end"].in_timezone(TZ)
    return "restore_test" if d.day_of_week == 0 or context["dag_run"].external_trigger else "skip_restore_test"


with DAG(
    dag_id=DAG_ID,
    description="Backup database + raw zone, retensi 30 hari, uji restore mingguan",
    schedule="0 12 * * *",            # 12:00 WITA
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=runlog.on_failure_dag_callback,
    tags=["orbit", "backup", "dr"],
    doc_md=__doc__,
) as dag:
    T = lambda tid, fn: PythonOperator(task_id=tid, python_callable=run(fn, **P))
    db_backup = T("backup_database", "pipeline.ops.backup.backup_database")
    raw = T("mirror_raw_zone", "pipeline.ops.backup.mirror_raw")
    retention = T("retention_cleanup", "pipeline.ops.backup.retention_cleanup")
    branch = BranchPythonOperator(task_id="weekly_restore_branch", python_callable=_is_sunday)
    restore = T("restore_test", "pipeline.ops.backup.restore_test")
    skip = EmptyOperator(task_id="skip_restore_test")

    db_backup >> raw >> [retention, branch]
    branch >> [restore, skip]
