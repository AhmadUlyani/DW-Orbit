"""Uji webhook alert tanpa Discord asli: server HTTP lokal menangkap payload.

    docker compose run --rm --no-deps airflow-scheduler python -m tests.integration_webhook

Memeriksa: (1) format embed Discord untuk URL discord.com/api/webhooks, (2) format generik {'text'},
(3) alert dengan notify=False tidak dikirim, (4) quality gate BLOCK mengirim notifikasi.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from pipeline import config, db, runlog
from pipeline.quality.checks import QualityGateBlocked, quality_gate

received = []


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        received.append((self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
        self.send_response(204); self.end_headers()

    def log_message(self, *a):
        pass


def main():
    srv = HTTPServer(("127.0.0.1", 8099), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = "http://127.0.0.1:8099"
    try:
        config.ALERT_WEBHOOK_URL = f"{base}/discord.com/api/webhooks/123/TEST"
        runlog.alert("ERROR", "uji: task contoh gagal final", "test_webhook_dag", "task_x", "run_1", notify=True)
        p = received[-1][1]
        assert p["username"] == "Orbit Pipeline" and p["embeds"][0]["title"].startswith("[ERROR] test_webhook_dag")
        print("[PASS] format embed Discord:", p["embeds"][0]["title"])

        n = len(received)
        runlog.alert("ERROR", "uji: kegagalan yang akan di-retry", "test_webhook_dag", "task_x", notify=False)
        assert len(received) == n, "alert notify=False tidak boleh dikirim"
        print("[PASS] alert tanpa notify (retry) tidak dikirim ke webhook")

        try:
            quality_gate(-1, "test_webhook_source", 100, 50)
        except QualityGateBlocked:
            pass
        assert "Quality gate BLOCK" in received[-1][1]["embeds"][0]["description"]
        print("[PASS] quality gate BLOCK mengirim notifikasi")

        config.ALERT_WEBHOOK_URL = f"{base}/generic"
        runlog.alert("CRITICAL", "uji: DAG gagal", "test_webhook_dag", notify=True)
        assert received[-1][0] == "/generic" and "text" in received[-1][1]
        print("[PASS] format webhook generik (Slack/Teams):", received[-1][1]["text"].splitlines()[0])
    finally:
        srv.shutdown()
        db.execute("DELETE FROM meta.alert WHERE dag_id='test_webhook_dag' OR message LIKE '%%test_webhook_source%%'")
        db.execute("DELETE FROM meta.quality_gate WHERE source='test_webhook_source'")


if __name__ == "__main__":
    main()
