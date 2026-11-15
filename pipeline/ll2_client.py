"""Client Launch Library 2 API (The Space Devs).

ApiClient       : HTTP GET + retry eksponensial (maks 3 percobaan) + log tiap request
ThrottleGuard   : menjaga batas 15 request/jam (cek /api-throttle/ sebelum request)
WatermarkStore  : waktu ekstraksi terakhir untuk ekstraksi inkremental
PagedFetcher    : mengikuti field `next` dengan checkpoint per halaman, jadi ingestion bisa
                  dilanjutkan kalau kuota habis atau proses gagal di tengah jalan
"""
import time

import requests

from . import config, db
from .runlog import log


class QuotaExhausted(RuntimeError):
    """Kuota API habis dan waktu tunggu melebihi batas yang diizinkan."""


class SchemaChanged(RuntimeError):
    """Struktur respons API berbeda dari yang diharapkan."""


class ThrottleGuard:
    def __init__(self, session, max_wait_s=3700):
        self.session = session
        self.max_wait_s = max_wait_s

    def status(self):
        r = self.session.get(f"{config.LL2_BASE_URL}/api-throttle/", timeout=30)
        r.raise_for_status()
        d = r.json()
        return d["your_request_limit"], d["current_use"], d["next_use_secs"]

    def acquire(self, needed=1):
        """Tunggu sampai kuota tersedia; QuotaExhausted kalau harus menunggu melebihi max_wait_s."""
        waited = 0
        while True:
            limit, used, next_secs = self.status()
            remaining = limit - used
            if remaining >= needed:
                return remaining
            wait = max(int(next_secs), 30) + 5
            if waited + wait > self.max_wait_s:
                raise QuotaExhausted(f"kuota LL2 habis ({used}/{limit}), tersedia lagi {next_secs}s")
            log.info("ThrottleGuard: kuota %s/%s terpakai, menunggu %ss", used, limit, wait)
            time.sleep(wait)
            waited += wait


class ApiClient:
    RETRY_STATUS = {429, 500, 502, 503, 504}

    def __init__(self, batch=None, max_attempts=3, backoff_s=10, max_wait_s=3700):
        self.batch = batch
        self.max_attempts = max_attempts
        self.backoff_s = backoff_s
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "orbit-nusantara-dw/1.0 (academic project)"
        if config.LL2_API_TOKEN:
            self.session.headers["Authorization"] = f"Token {config.LL2_API_TOKEN}"
        self.throttle = ThrottleGuard(self.session, max_wait_s=max_wait_s)

    def _log(self, endpoint, url, status, attempt, ms, size, err=None):
        try:
            db.execute("""INSERT INTO meta.api_request_log (batch_id, endpoint, url, http_status, attempt,
                              duration_ms, bytes, error) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                       (self.batch.batch_id if self.batch else None, endpoint, url, status, attempt,
                        ms, size, err))
        except Exception:
            log.exception("gagal menulis api_request_log")

    def get(self, url, params=None, endpoint=""):
        last_exc = None
        for attempt in range(1, self.max_attempts + 1):
            self.throttle.acquire(1)
            t0 = time.perf_counter()
            try:
                r = self.session.get(url, params=params, timeout=(15, 120))
                ms = int((time.perf_counter() - t0) * 1000)
                self._log(endpoint, r.url, r.status_code, attempt, ms, len(r.content))
                if self.batch:
                    self.batch.api_requests += 1
                if r.status_code in self.RETRY_STATUS:
                    raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
                r.raise_for_status()
                return r.json()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status and status not in self.RETRY_STATUS:   # 4xx lain: tidak di-retry
                    raise
                last_exc = exc
                ms = int((time.perf_counter() - t0) * 1000)
                if not status:
                    self._log(endpoint, url, None, attempt, ms, None, f"{type(exc).__name__}: {exc}")
                if attempt < self.max_attempts:
                    sleep = self.backoff_s * (3 ** (attempt - 1))      # 10s, 30s, 90s
                    log.warning("request gagal (%s), retry %s/%s dalam %ss", exc, attempt,
                                self.max_attempts, sleep)
                    time.sleep(sleep)
        raise RuntimeError(f"request LL2 gagal setelah {self.max_attempts} percobaan: {last_exc}")


def validate_page(page, required_item_keys):
    """Cek struktur halaman sebelum ditulis ke Data Lake."""
    for k in ("count", "next", "results"):
        if k not in page:
            raise SchemaChanged(f"field '{k}' tidak ada pada respons halaman")
    for item in page["results"]:
        missing = [k for k in required_item_keys if k not in item]
        if missing:
            raise SchemaChanged(f"item {item.get('id')} tidak memiliki field {missing}")


class WatermarkStore:
    @staticmethod
    def get(name, default=None):
        v = db.scalar("SELECT value FROM meta.watermark WHERE name=%s", (name,))
        return v if v is not None else default

    @staticmethod
    def set(name, value):
        db.execute("""INSERT INTO meta.watermark (name, value) VALUES (%s,%s)
                      ON CONFLICT (name) DO UPDATE SET value=EXCLUDED.value, updated_at=now()""",
                   (name, str(value)))


class PagedFetcher:
    """Mengambil seluruh halaman sebuah endpoint dengan checkpoint per halaman."""

    def __init__(self, client: ApiClient, job, endpoint_label, required_keys):
        self.client, self.job, self.endpoint = client, job, endpoint_label
        self.required_keys = required_keys

    def run(self, first_url, params, on_page):
        row = db.execute("SELECT next_url, pages_done, records_done, status FROM meta.ingest_checkpoint "
                         "WHERE job=%s", (self.job,), fetch=True)
        if row and row[0][3] == "DONE":
            log.info("checkpoint %s sudah DONE, dilewati", self.job)
            return 0, 0
        if row and row[0][0]:
            url, prm, pages, records = row[0][0], None, row[0][1], row[0][2]
            log.info("melanjutkan %s dari halaman %s", self.job, pages + 1)
        else:
            url, prm, pages, records = first_url, params, 0, 0
        new_pages = 0
        while url:
            page = self.client.get(url, params=prm, endpoint=self.endpoint)
            validate_page(page, self.required_keys)
            pages += 1
            new_pages += 1
            records += len(page["results"])
            on_page(pages, page)
            url, prm = page["next"], None
            db.execute("""INSERT INTO meta.ingest_checkpoint (job, next_url, pages_done, records_done,
                              total_count, status) VALUES (%s,%s,%s,%s,%s,%s)
                          ON CONFLICT (job) DO UPDATE SET next_url=EXCLUDED.next_url,
                              pages_done=EXCLUDED.pages_done, records_done=EXCLUDED.records_done,
                              total_count=EXCLUDED.total_count, status=EXCLUDED.status, updated_at=now()""",
                       (self.job, url, pages, records, page["count"], "DONE" if not url else "IN_PROGRESS"))
        return new_pages, records
