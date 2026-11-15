"""DAG ETL harian (12:30 WITA).

Sumber -> Data Lake (raw) -> staging raw -> transform + quality gate -> staging clean
       -> dimensi -> fakta -> validasi DWH -> export Silver/Gold -> skor ML jadwal

Dalam satu run, task hilir hanya jalan kalau task hulunya sukses. Antar hari tidak saling menunggu
(depends_on_past=False) karena snapshot jadwal tidak bisa diambil ulang dari API: kalau satu hari gagal
menghentikan hari berikutnya, snapshot hari-hari itu hilang. Pengamannya:
  1. tiap tahap idempotent (checksum, checkpoint paging, watermark, upsert, unique constraint);
  2. quality gate menolak data buruk masuk DWH;
  3. transform_ll2_upcoming memproses semua snapshot staging yang belum masuk DWH, urut dari tanggal
     paling lama, jadi snapshot yang gagal di transform/load ikut terproses di run berikutnya;
  4. kegagalan dikirim sebagai alert (meta.alert + webhook Discord).

Kalau laptop baru menyala setelah 12:30, scheduler (catchup=False) membuat satu run susulan.
guard_once_per_day men-skip run terjadwal kalau ETL sudah sukses di hari (WITA) yang sama.
Trigger manual selalu jalan.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

from orbit_common import DEFAULT_ARGS, START, run, runlog

DAG_ID = "orbit_daily_etl"


def _guard_day(**context):
    from pipeline.ops.guard import not_yet_ran_today   # import lazy, lihat orbit_common
    return not_yet_ran_today(DAG_ID, force=bool(context["dag_run"].external_trigger))

with DAG(
    dag_id=DAG_ID,
    description="ETL harian: CSV internal + Launch Library 2 -> Data Lake -> DWH -> ML scoring",
    schedule="30 12 * * *",           # 12:30 WITA
    start_date=START,
    catchup=False,                      # hanya 1 run susulan (jadwal terakhir), bukan semua hari yang terlewat
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "depends_on_past": False},   # kegagalan kemarin tidak memblokir hari ini
    on_success_callback=runlog.on_success_dag_callback,
    on_failure_callback=runlog.on_failure_dag_callback,
    sla_miss_callback=runlog.sla_miss_callback,
    dagrun_timeout=timedelta(hours=3),
    tags=["orbit", "etl", "daily"],
    doc_md=__doc__,
) as dag:
    start = ShortCircuitOperator(
        task_id="guard_once_per_day",
        python_callable=_guard_day,
        ignore_downstream_trigger_rules=True, retries=1)

    with TaskGroup("extract_ingest", tooltip="Sumber -> Data Lake raw zone") as extract:
        ingest_csv = PythonOperator(task_id="ingest_maven_csv",
                                    python_callable=run("pipeline.ingest.maven.ingest_maven", pipeline=DAG_ID))
        ingest_upcoming = PythonOperator(task_id="ingest_ll2_upcoming",
                                         python_callable=run("pipeline.ingest.ll2.ingest_upcoming", pipeline=DAG_ID),
                                         execution_timeout=timedelta(hours=2))
        ingest_previous = PythonOperator(task_id="ingest_ll2_previous_incremental",
                                         python_callable=run("pipeline.ingest.ll2.ingest_previous", pipeline=DAG_ID),
                                         execution_timeout=timedelta(hours=2))
        ingest_upcoming >> ingest_previous          # berurutan agar kuota 15 request/jam tidak berebut

    with TaskGroup("stage_raw", tooltip="Data Lake -> staging raw (jsonb / text)") as stage:
        stage_csv = PythonOperator(task_id="stage_maven_raw",
                                   python_callable=run("pipeline.ingest.maven.stage_maven", pipeline=DAG_ID))
        stage_launch = PythonOperator(task_id="stage_ll2_launch_raw",
                                      python_callable=run("pipeline.ingest.ll2.stage_launches", pipeline=DAG_ID))
        ingest_new_cfg = PythonOperator(task_id="ingest_ll2_configs_new",
                                        python_callable=run("pipeline.ingest.ll2.ingest_configs_new", pipeline=DAG_ID))
        stage_cfg = PythonOperator(task_id="stage_ll2_config_raw",
                                   python_callable=run("pipeline.ingest.ll2.stage_configs", pipeline=DAG_ID))
        stage_launch >> ingest_new_cfg >> stage_cfg

    with TaskGroup("transform_quality", tooltip="Cleaning, validation, standardization, quality gate") as transform:
        t_maven = PythonOperator(task_id="transform_maven",
                                 python_callable=run("pipeline.transform.maven.transform_maven", pipeline=DAG_ID))
        t_cfg = PythonOperator(task_id="transform_ll2_configs",
                               python_callable=run("pipeline.transform.ll2.transform_configs", pipeline=DAG_ID))
        t_launch = PythonOperator(task_id="transform_ll2_launches",
                                  python_callable=run("pipeline.transform.ll2.transform_launches", pipeline=DAG_ID))
        t_upcoming = PythonOperator(task_id="transform_ll2_upcoming",
                                    python_callable=run("pipeline.transform.ll2.transform_upcoming", pipeline=DAG_ID))
        # arsip internal & konfigurasi diproses dulu: nama baku roket/situs dari arsip dipakai untuk
        # mencocokkan nama LL2, dan biaya roket dari konfigurasi dipakai untuk melengkapi harga peluncuran
        [t_maven, t_cfg] >> t_launch >> t_upcoming

    with TaskGroup("load_dwh", tooltip="Dimensi (SCD1/SCD2) lalu fakta") as load:
        l_dim = PythonOperator(task_id="load_dimensions",
                               python_callable=run("pipeline.load.warehouse.load_dimensions", pipeline=DAG_ID))
        l_fact = PythonOperator(task_id="load_facts",
                                python_callable=run("pipeline.load.warehouse.load_facts", pipeline=DAG_ID),
                                sla=timedelta(hours=2))
        l_dim >> l_fact

    validate = PythonOperator(task_id="validate_dw",
                              python_callable=run("pipeline.quality.dw_checks.validate_dw", pipeline=DAG_ID),
                              retries=0)       # kegagalan validasi bukan masalah transien -> tidak di-retry

    with TaskGroup("publish", tooltip="Export Silver/Gold + skor ML jadwal") as publish:
        export = PythonOperator(task_id="export_lake_silver_gold",
                                python_callable=run("pipeline.ops.export_lake.export_lake", pipeline=DAG_ID))
        score = PythonOperator(task_id="ml_score_upcoming",
                               python_callable=run("pipeline.ml.score.score_upcoming", pipeline=DAG_ID))

    end = EmptyOperator(task_id="end")

    start >> extract
    ingest_csv >> stage_csv >> t_maven
    ingest_previous >> stage_launch
    stage_cfg >> t_cfg
    transform >> load >> validate >> publish >> end
