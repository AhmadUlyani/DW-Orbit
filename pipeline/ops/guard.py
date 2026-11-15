"""Guard sekali per hari (ETL harian) dan sekali per minggu (snapshot konfigurasi roket).

Dengan catchup=False, Airflow membuat satu run susulan saat scheduler baru menyala. Kalau laptop menyala
pukul 11:00, run susulan kemarin jalan lalu jadwal 12:30 hari ini ikut jalan. Guard mencegah run kedua
di hari (WITA) yang sama. Return False membuat ShortCircuitOperator men-skip task berikutnya.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from .. import db
from ..runlog import log

WITA = ZoneInfo("Asia/Makassar")


def not_yet_ran_today(dag_id, marker_task="load_facts", force=False, **_):
    """Trigger manual (force=True) selalu lolos."""
    today = datetime.now(WITA).date()
    if force:
        log.info("run manual -> guard harian dilewati (%s, WITA)", today)
        return True
    n = db.scalar("""SELECT COUNT(*) FROM meta.etl_batch
                     WHERE pipeline = %s AND task = %s AND status = 'SUCCESS'
                       AND (started_at AT TIME ZONE 'Asia/Makassar')::date = %s""",
                  (dag_id, marker_task, today))
    if n:
        log.info("%s sudah sukses %s kali pada %s (WITA) -> run ini di-skip agar tidak dobel", dag_id, n, today)
        db.execute("""INSERT INTO meta.alert (level, dag_id, task_id, message) VALUES ('INFO', %s, 'guard_once_per_day',
                      %s)""", (dag_id, f"Run di-skip: {dag_id} sudah sukses hari ini ({today})"))
        return False
    log.info("%s belum sukses pada %s (WITA) -> lanjut", dag_id, today)
    return True


def no_config_snapshot_this_week(dag_id="orbit_weekly_config", force=False, **_):
    """Cek dari data, bukan log DAG, karena snapshot juga bisa dibuat orbit_init/orbit_reprocess.

    Trigger manual (force=True) selalu lolos.
    """
    today = datetime.now(WITA).date()
    iso = today.isocalendar()
    last = db.scalar("""SELECT MAX(t.tanggal) FROM dw.fact_konfigurasi_roket f
                        JOIN dw.dim_tanggal t ON t.date_key = f.snapshot_date_key""")
    if force:
        log.info("run manual -> guard mingguan dilewati (snapshot terakhir %s)", last)
        return True
    if last is not None and last.isocalendar()[:2] == iso[:2]:
        log.info("snapshot konfigurasi minggu ISO %s-W%02d sudah ada (%s) -> run di-skip", iso[0], iso[1], last)
        db.execute("""INSERT INTO meta.alert (level, dag_id, task_id, message) VALUES ('INFO', %s, 'guard_once_per_week',
                      %s)""", (dag_id, f"Run di-skip: snapshot konfigurasi minggu ini sudah ada ({last})"))
        return False
    log.info("belum ada snapshot konfigurasi minggu ini (terakhir %s) -> lanjut", last)
    return True
