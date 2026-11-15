"""Logging operasional, status batch, alert, dan callback Airflow.

Setiap task dibungkus `track_batch(...)` yang membuat baris meta.etl_batch (RUNNING), lalu mengisinya
SUCCESS beserta durasi dan jumlah baris, atau FAILED beserta traceback. Exception dilempar ulang
supaya Airflow bisa retry.
"""
import json
import logging
import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone

import requests

from . import config, db

log = logging.getLogger("orbit")
if not log.handlers:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


class Batch:
    def __init__(self, batch_id, pipeline, task, source):
        self.batch_id, self.pipeline, self.task, self.source = batch_id, pipeline, task, source
        self.rows_read = self.rows_written = self.rows_rejected = self.api_requests = 0
        self.status = "SUCCESS"
        self.message = None

    def note(self, msg):
        log.info("[batch %s] %s", self.batch_id, msg)
        self.message = msg if not self.message else f"{self.message} | {msg}"


def _run_context():
    """Ambil dag_run_id & try_number dari env Airflow bila tersedia."""
    return (os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or "manual",
            int(os.getenv("AIRFLOW_CTX_TRY_NUMBER", "1") or 1))


@contextmanager
def track_batch(pipeline, task, source=None):
    run_id, attempt = _run_context()
    batch_id = db.scalar(
        """INSERT INTO meta.etl_batch (pipeline, task, run_id, source, attempt)
           VALUES (%s,%s,%s,%s,%s) RETURNING batch_id""",
        (pipeline, task, run_id, source, attempt))
    b = Batch(batch_id, pipeline, task, source)
    t0 = time.perf_counter()
    log.info("[batch %s] START %s.%s source=%s", batch_id, pipeline, task, source)
    try:
        yield b
    except Exception as exc:
        dur = time.perf_counter() - t0
        db.execute(
            """UPDATE meta.etl_batch SET status='FAILED', finished_at=now(), duration_s=%s,
                   rows_read=%s, rows_written=%s, rows_rejected=%s, api_requests=%s,
                   message=%s, error_trace=%s WHERE batch_id=%s""",
            (round(dur, 3), b.rows_read, b.rows_written, b.rows_rejected, b.api_requests,
             f"{type(exc).__name__}: {exc}"[:2000], traceback.format_exc()[-8000:], batch_id))
        # hanya dicatat; webhook dikirim callback Airflow setelah retry habis
        alert("ERROR", f"{pipeline}.{task} gagal (batch {batch_id}): {exc}", pipeline, task, run_id)
        log.exception("[batch %s] FAILED", batch_id)
        raise
    else:
        dur = time.perf_counter() - t0
        db.execute(
            """UPDATE meta.etl_batch SET status=%s, finished_at=now(), duration_s=%s,
                   rows_read=%s, rows_written=%s, rows_rejected=%s, api_requests=%s, message=%s
               WHERE batch_id=%s""",
            (b.status, round(dur, 3), b.rows_read, b.rows_written, b.rows_rejected,
             b.api_requests, (b.message or "")[:2000], batch_id))
        log.info("[batch %s] %s in %.2fs read=%s written=%s rejected=%s api=%s", batch_id,
                 b.status, dur, b.rows_read, b.rows_written, b.rows_rejected, b.api_requests)


def alert(level, message, dag_id=None, task_id=None, run_id=None, notify=False):
    """Simpan alert ke meta.alert; kalau notify=True dan ALERT_WEBHOOK_URL diisi, kirim juga ke webhook.

    notify=True dipakai untuk task gagal setelah retry habis, quality gate BLOCK, dan DAG run gagal.
    Percobaan yang masih akan di-retry hanya dicatat.
    """
    try:
        db.execute("INSERT INTO meta.alert (level, dag_id, task_id, run_id, message) VALUES (%s,%s,%s,%s,%s)",
                   (level, dag_id, task_id, run_id, message[:4000]))
    except Exception:  # alert tidak boleh menggagalkan pipeline
        log.exception("gagal menulis meta.alert")
    if notify and config.ALERT_WEBHOOK_URL:
        send_webhook(level, message, dag_id, task_id, run_id)
    log.warning("ALERT %s: %s", level, message)


_COLORS = {"INFO": 0x3498DB, "WARNING": 0xF39C12, "ERROR": 0xE74C3C, "CRITICAL": 0x8E0000}


def send_webhook(level, message, dag_id=None, task_id=None, run_id=None, url=None):
    """Kirim notifikasi ke Discord (embed) atau webhook generik (field 'text').

    Kegagalan kirim hanya dicatat di log.
    """
    url = url or config.ALERT_WEBHOOK_URL
    if not url:
        return False
    title = f"[{level}] {dag_id or 'orbit'}" + (f" · {task_id}" if task_id else "")
    if "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url:
        fields = [{"name": "Run", "value": str(run_id)[:1000], "inline": False}] if run_id else []
        payload = {"username": "Orbit Pipeline",
                   "embeds": [{"title": title[:250], "description": message[:4000],
                               "color": _COLORS.get(level, 0x95A5A6), "fields": fields,
                               "timestamp": datetime.now(timezone.utc).isoformat(),
                               "footer": {"text": "orbit-bigdata · Airflow"}}]}
    else:
        payload = {"text": f"{title}: {message}"}
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 429:                       # rate limit Discord
                time.sleep(min(float(r.json().get("retry_after", 2)), 10))
                continue
            r.raise_for_status()
            return True
        except Exception:
            log.exception("gagal mengirim webhook alert (percobaan %s)", attempt + 1)
            time.sleep(2)
    return False


# Airflow callbacks
def on_failure_callback(context):
    """Dipanggil Airflow saat task gagal setelah semua retry habis."""
    ti = context.get("task_instance")
    alert("ERROR", f"Task {ti.task_id} pada DAG {ti.dag_id} GAGAL setelah {ti.try_number} percobaan: "
          f"{context.get('exception')}", ti.dag_id, ti.task_id, context.get("run_id"), notify=True)


def on_retry_callback(context):
    ti = context.get("task_instance")
    alert("WARNING", f"Task {ti.task_id} di-retry (percobaan {ti.try_number}): {context.get('exception')}",
          ti.dag_id, ti.task_id, context.get("run_id"))


def on_success_dag_callback(context):
    dr = context.get("dag_run")
    alert("INFO", f"DAG {dr.dag_id} selesai SUKSES (run {dr.run_id})", dr.dag_id, None, dr.run_id)


def on_failure_dag_callback(context):
    dr = context.get("dag_run")
    alert("CRITICAL", f"DAG {dr.dag_id} GAGAL (run {dr.run_id}): {context.get('reason')}",
          dr.dag_id, None, dr.run_id, notify=True)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    alert("WARNING", f"SLA terlewati pada DAG {dag.dag_id}: {task_list}", dag.dag_id)


def write_lineage(batch_id, target_table, layer, source_refs, rules, rows):
    db.execute("""INSERT INTO meta.lineage (batch_id, target_table, target_layer, source_refs,
                      rules_applied, rows_affected) VALUES (%s,%s,%s,%s,%s,%s)""",
               (batch_id, target_table, layer, list(source_refs), list(rules), rows))


def dumps(o):
    return json.dumps(o, default=str, ensure_ascii=False)
