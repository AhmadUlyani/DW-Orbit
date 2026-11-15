"""DAG Machine Learning (Senin 14:00 WITA + dipicu setelah orbit_init).

Data dibaca dari mart.v_peluncuran.
  train_launch_risk     : klasifikasi risiko kegagalan (baseline vs LogReg vs RF vs HistGB)
  train_launch_forecast : forecasting jumlah peluncuran bulanan (naive vs seasonal vs Ridge vs HistGB)
  data_drift_psi        : PSI fitur antar periode / antar sumber
  ml_score_upcoming     : skor risiko untuk jadwal snapshot terbaru dengan champion baru
"""
from airflow import DAG
from airflow.operators.python import PythonOperator

from orbit_common import DEFAULT_ARGS, START, run, runlog

DAG_ID = "orbit_ml_training"
P = dict(pipeline=DAG_ID)


def _train_and_drift(**context):
    from pipeline.ml import drift, train_risk
    version = train_risk.train(pipeline=DAG_ID)
    drift.run(version, pipeline=DAG_ID)


with DAG(
    dag_id=DAG_ID,
    description="Training & evaluasi model risiko kegagalan + forecasting, drift, scoring",
    schedule="0 14 * * 1",            # Senin 14:00 WITA
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    on_failure_callback=runlog.on_failure_dag_callback,
    tags=["orbit", "ml"],
    doc_md=__doc__,
) as dag:
    risk = PythonOperator(task_id="train_launch_risk_and_drift", python_callable=_train_and_drift)
    fc = PythonOperator(task_id="train_launch_forecast", python_callable=run("pipeline.ml.forecast.train", **P))
    score = PythonOperator(task_id="ml_score_upcoming", python_callable=run("pipeline.ml.score.score_upcoming", **P))
    [risk, fc] >> score
