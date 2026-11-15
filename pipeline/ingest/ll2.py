"""Ingestion Launch Library 2 API v2.3.0.

Endpoint:
  /launches/previous/        muatan awal (net > cutoff arsip) lalu inkremental (last_updated >= watermark)
  /launches/upcoming/        diambil penuh setiap hari -> 1 snapshot jadwal per hari
  /launcher_configurations/  penuh mingguan (mode=detailed) + per-id untuk roket baru (harian)

Setiap halaman respons disimpan apa adanya ke raw zone (JSON gzip), baru dimuat ke staging,
sehingga data bisa diproses ulang kalau transformasi gagal.
"""
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .. import config, db, lake
from ..ll2_client import ApiClient, PagedFetcher, WatermarkStore
from ..runlog import track_batch, write_lineage, log

WITA = ZoneInfo("Asia/Makassar")
LAUNCH_KEYS = ["id", "name", "net", "status", "last_updated", "launch_service_provider", "rocket", "pad"]
CONFIG_KEYS = ["id", "full_name", "active", "manufacturer", "launch_cost", "total_launch_count"]
WM_PREVIOUS = "ll2_previous_last_updated"


def today_wita() -> date:
    return datetime.now(WITA).date()


def _saver(b, dataset, ingest_date):
    def on_page(page_no, page):
        lake.put_json_gz("raw", lake.raw_key("ll2", dataset, ingest_date, b.batch_id, f"page_{page_no:04d}.json.gz"),
                         page, source="ll2", dataset=dataset, batch_id=b.batch_id,
                         ingest_date=ingest_date, record_count=len(page["results"]))
        b.rows_read += len(page["results"])
        b.rows_written += len(page["results"])
    return on_page


def ingest_upcoming(snapshot_date: date | None = None, pipeline="orbit_daily_etl", max_wait_s=3700):
    snapshot_date = snapshot_date or today_wita()
    with track_batch(pipeline, "ingest_ll2_upcoming", "ll2_api") as b:
        client = ApiClient(b, max_wait_s=max_wait_s)
        f = PagedFetcher(client, f"ll2_upcoming@{snapshot_date}", "launches/upcoming", LAUNCH_KEYS)
        pages, recs = f.run(f"{config.LL2_BASE_URL}/launches/upcoming/",
                            {"limit": config.LL2_PAGE_LIMIT, "mode": "normal", "ordering": "net"},
                            _saver(b, "launches_upcoming", snapshot_date))
        b.note(f"snapshot {snapshot_date}: {pages} halaman baru, total {recs} jadwal")
        return recs


def ingest_previous(pipeline="orbit_daily_etl", max_wait_s=3700):
    """Muatan awal penuh (sekali) lalu inkremental berdasarkan watermark last_updated."""
    wm = WatermarkStore.get(WM_PREVIOUS)
    ingest_date = today_wita()
    with track_batch(pipeline, "ingest_ll2_previous", "ll2_api") as b:
        client = ApiClient(b, max_wait_s=max_wait_s)
        params = {"limit": config.LL2_PAGE_LIMIT, "mode": "normal",
                  "net__gt": config.ARCHIVE_CUTOFF_UTC}           # TI-01: tidak tumpang tindih arsip
        if wm is None:
            job, params["ordering"] = "ll2_previous@initial", "net"
        else:
            job = f"ll2_previous@{ingest_date}"
            params["last_updated__gte"], params["ordering"] = wm, "last_updated"
        start_key = f"{WM_PREVIOUS}__pending__{job}"
        if WatermarkStore.get(start_key) is None:
            # watermark baru = waktu mulai job dikurangi 1 jam, supaya perubahan selama job berjalan
            # terambil lagi di run berikutnya
            WatermarkStore.set(start_key, (datetime.now(timezone.utc) - timedelta(hours=1))
                               .strftime("%Y-%m-%dT%H:%M:%SZ"))
        f = PagedFetcher(client, job, "launches/previous", LAUNCH_KEYS)
        pages, recs = f.run(f"{config.LL2_BASE_URL}/launches/previous/", params,
                            _saver(b, "launches_previous", ingest_date))
        new_wm = WatermarkStore.get(start_key)
        WatermarkStore.set(WM_PREVIOUS, new_wm)
        b.note(f"{job}: {pages} halaman baru, {recs} record; watermark -> {new_wm}")
        return recs


def ingest_configs_full(snapshot_date: date | None = None, pipeline="orbit_weekly_config", max_wait_s=3700):
    snapshot_date = snapshot_date or today_wita()
    with track_batch(pipeline, "ingest_ll2_configs_full", "ll2_api") as b:
        client = ApiClient(b, max_wait_s=max_wait_s)
        f = PagedFetcher(client, f"ll2_config@{snapshot_date}", "launcher_configurations", CONFIG_KEYS)
        pages, recs = f.run(f"{config.LL2_BASE_URL}/launcher_configurations/",
                            {"limit": config.LL2_PAGE_LIMIT, "mode": "detailed", "ordering": "id"},
                            _saver(b, "launcher_configurations", snapshot_date))
        b.note(f"snapshot konfigurasi {snapshot_date}: {recs} roket")
        return recs


def ingest_configs_new(pipeline="orbit_daily_etl", max_requests=3, max_wait_s=600):
    """Ambil konfigurasi roket (per id) yang ada di data peluncuran tapi belum ada di staging."""
    ingest_date = today_wita()
    with track_batch(pipeline, "ingest_ll2_configs_new", "ll2_api") as b:
        missing = [r[0] for r in db.execute(
            """SELECT DISTINCT (payload->'rocket'->'configuration'->>'id')::int AS cid
               FROM staging.stg_ll2_launch_raw
               WHERE (payload->'rocket'->'configuration'->>'id')::int NOT IN
                     (SELECT DISTINCT ll2_config_id FROM staging.stg_ll2_config_raw)
               ORDER BY 1""", fetch=True)]
        if not missing:
            b.status = "SKIPPED"
            b.note("tidak ada konfigurasi roket baru")
            return 0
        client = ApiClient(b, max_wait_s=max_wait_s)
        for cid in missing[:max_requests]:
            obj = client.get(f"{config.LL2_BASE_URL}/launcher_configurations/{cid}/", endpoint="launcher_configurations/id")
            page = {"count": 1, "next": None, "previous": None, "results": [obj]}
            _saver(b, "launcher_configurations_new", ingest_date)(cid, page)
        if len(missing) > max_requests:
            b.status = "PARTIAL"
            b.note(f"{len(missing) - max_requests} id sisa diambil pada run berikutnya/mingguan")
        return min(len(missing), max_requests)


# Data Lake (raw) -> staging raw (jsonb), hanya objek yang belum pernah dimuat.
def _pending_objects(dataset, table):
    return db.execute(
        f"""SELECT o.object_key, o.ingest_date, o.batch_id FROM meta.lake_object o
            WHERE o.zone='raw' AND o.dataset=%s
              AND NOT EXISTS (SELECT 1 FROM {table} s WHERE s.source_object = o.object_key)
            ORDER BY o.created_at""", (dataset,), fetch=True)


def stage_launches(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "stage_ll2_launch_raw", "ll2_api") as b:
        objs = [("previous", *o) for o in _pending_objects("launches_previous", "staging.stg_ll2_launch_raw")]
        objs += [("upcoming", *o) for o in _pending_objects("launches_upcoming", "staging.stg_ll2_launch_raw")]
        total = 0
        with db.transaction() as conn, conn.cursor() as cur:
            for endpoint, key, ingest_date, _ in objs:
                page = lake.get_json_gz(key)
                rows = [(b.batch_id, endpoint, it["id"], it.get("last_updated"), ingest_date,
                         db.json.dumps(it), key) for it in page["results"]]
                db.psycopg2.extras.execute_values(
                    cur, """INSERT INTO staging.stg_ll2_launch_raw (batch_id, endpoint, ll2_launch_id,
                            last_updated, snapshot_date, payload, source_object) VALUES %s""", rows)
                total += len(rows)
        b.rows_read = b.rows_written = total
        b.note(f"{len(objs)} objek lake -> {total} baris staging")
        if objs:
            write_lineage(b.batch_id, "staging.stg_ll2_launch_raw", "staging", [o[1] for o in objs],
                          ["EXTRACT-AS-IS"], total)
        return total


def stage_configs(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "stage_ll2_config_raw", "ll2_api") as b:
        objs = (_pending_objects("launcher_configurations", "staging.stg_ll2_config_raw")
                + _pending_objects("launcher_configurations_new", "staging.stg_ll2_config_raw"))
        total = 0
        with db.transaction() as conn, conn.cursor() as cur:
            for key, ingest_date, _ in objs:
                page = lake.get_json_gz(key)
                rows = [(b.batch_id, it["id"], ingest_date, db.json.dumps(it), key) for it in page["results"]]
                db.psycopg2.extras.execute_values(
                    cur, """INSERT INTO staging.stg_ll2_config_raw (batch_id, ll2_config_id, snapshot_date,
                            payload, source_object) VALUES %s""", rows)
                total += len(rows)
        b.rows_read = b.rows_written = total
        b.note(f"{len(objs)} objek lake -> {total} baris staging")
        if objs:
            write_lineage(b.batch_id, "staging.stg_ll2_config_raw", "staging", [o[0] for o in objs],
                          ["EXTRACT-AS-IS"], total)
        return total
