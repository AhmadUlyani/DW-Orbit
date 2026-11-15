"""Konfigurasi bersama untuk semua file DAG (file ini sendiri bukan DAG).

Scheduler mem-parse file DAG tiap 30 detik, jadi modul berat dari package `pipeline` (pandas,
psycopg2, scikit-learn) tidak diimpor di level atas. Import di level atas membuat parsing lambat dan
pernah memicu error 'partially initialized module pandas'. Fungsi pipeline dan callback baru diimpor
saat task atau callback berjalan.
"""
from datetime import timedelta

import pendulum

# Semua jadwal memakai WITA (Asia/Makassar, UTC+8):
# backup 12:00, ETL 12:30, konfigurasi Minggu 13:30, ML Senin 14:00.
TZ = pendulum.timezone("Asia/Makassar")
START = pendulum.datetime(2026, 11, 1, tz=TZ)


class runlog:  # noqa: N801
    """Meneruskan callback Airflow ke pipeline.runlog, diimpor saat callback dipanggil."""

    @staticmethod
    def _rl():
        from pipeline import runlog as rl
        return rl

    @staticmethod
    def on_failure_callback(context):
        runlog._rl().on_failure_callback(context)

    @staticmethod
    def on_retry_callback(context):
        runlog._rl().on_retry_callback(context)

    @staticmethod
    def on_success_dag_callback(context):
        runlog._rl().on_success_dag_callback(context)

    @staticmethod
    def on_failure_dag_callback(context):
        runlog._rl().on_failure_dag_callback(context)

    @staticmethod
    def sla_miss_callback(*args, **kwargs):
        runlog._rl().sla_miss_callback(*args, **kwargs)


DEFAULT_ARGS = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,           # jeda makin panjang: 2, 4, 8 menit
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": runlog.on_failure_callback,
    "on_retry_callback": runlog.on_retry_callback,
    "depends_on_past": False,
    "do_xcom_push": False,
}


def run(fn_path, **kwargs):
    """Bungkus fungsi pipeline (path string) jadi python_callable; modul diimpor saat task jalan."""
    def _call(**context):
        import importlib
        from airflow.exceptions import AirflowFailException
        from pipeline.quality.checks import QualityGateBlocked
        mod, fn = fn_path.rsplit(".", 1)
        try:
            return getattr(importlib.import_module(mod), fn)(**kwargs)
        except QualityGateBlocked as exc:
            # data buruk tidak akan membaik dengan retry, jadi task langsung gagal
            raise AirflowFailException(str(exc)) from exc
    _call.__name__ = fn_path.rsplit(".", 1)[1]
    return _call
