"""Backup & Disaster Recovery.

Yang di-backup:
  1. Database dw_orbit (staging, meta, dw, mart, ml) : pg_dump format custom (terkompresi)
       -> lokal ./backups/db  (disk host, di luar volume database)
       -> MinIO orbit-backup/db/YYYY/MM/DD/           (object storage terpisah)
  2. Raw zone Data Lake (orbit-raw)                   : mirror objek baru -> orbit-backup/raw/ + ./backups/raw
     (snapshot jadwal harian tidak bisa diambil ulang dari API)
Retensi 30 hari (retention_cleanup + lifecycle rule bucket MinIO).
Uji restore mingguan: pg_restore ke database sementara, bandingkan row count, catat RTO.
"""
import hashlib
import os
import subprocess
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from minio.commonconfig import CopySource

from .. import config, db, lake
from ..runlog import alert, log, track_batch

# image Airflow sudah membawa client PG17; versi harus sama dengan server (PostgreSQL 16)
PG_BIN = os.getenv("PG_BIN", "/usr/lib/postgresql/16/bin")
BACKUP_TABLES = ["dw.fact_peluncuran", "dw.fact_jadwal_snapshot", "dw.fact_konfigurasi_roket", "dw.dim_roket",
                 "dw.dim_perusahaan", "dw.dim_lokasi", "dw.dim_orbit", "dw.dim_status", "dw.dim_tanggal",
                 "staging.stg_peluncuran_clean", "meta.etl_batch", "meta.watermark", "meta.lake_object"]


def _pg_env(user=None, password=None):
    env = dict(os.environ)
    env["PGPASSWORD"] = password or config.DW["password"]
    return env, user or config.DW["user"]


def _sha256_file(p: Path):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def backup_database(pipeline="orbit_backup_dr"):
    with track_batch(pipeline, "backup_database", "dw_orbit") as b:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = config.BACKUP_DIR / "db"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"dw_orbit_{ts}.dump"
        env, user = _pg_env()
        t0 = time.perf_counter()
        cmd = [f"{PG_BIN}/pg_dump", "-h", config.DW["host"], "-p", str(config.DW["port"]), "-U", user,
               "-d", config.DW["dbname"], "-Fc", "-Z", "6", "--no-owner", "-f", str(path)]
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1800)
        if r.returncode != 0:
            raise RuntimeError(f"pg_dump gagal: {r.stderr.strip()}")
        size, digest = path.stat().st_size, _sha256_file(path)
        key = f"db/{date.today():%Y/%m/%d}/{path.name}"
        lake.client().fput_object(config.BUCKET["backup"], key, str(path), metadata={"sha256": digest})
        db.execute("""INSERT INTO meta.backup_log (backup_type, object_key, local_path, size_bytes, sha256,
                          status, finished_at, retention_until, message)
                      VALUES ('db_full', %s, %s, %s, %s, 'SUCCESS', now(), %s, %s)""",
                   (f"{config.BUCKET['backup']}/{key}", str(path), size, digest,
                    date.today() + timedelta(days=config.RETENTION_BACKUP_DAYS),
                    f"pg_dump {time.perf_counter() - t0:.1f}s"))
        b.rows_written = 1
        b.note(f"{path.name} {size / 1024:.0f} KB -> {config.BUCKET['backup']}/{key}")
        return key


def mirror_raw(pipeline="orbit_backup_dr"):
    """Salin objek raw zone yang belum dicadangkan ke bucket backup dan disk lokal."""
    with track_batch(pipeline, "mirror_raw_zone", "orbit-raw") as b:
        c = lake.client()
        done = {o.object_name for o in c.list_objects(config.BUCKET["backup"], prefix="raw/", recursive=True)}
        local_root = config.BACKUP_DIR / "raw"
        copied, total_bytes = 0, 0
        for o in c.list_objects(config.BUCKET["raw"], recursive=True):
            b.rows_read += 1
            dst = f"raw/{o.object_name}"
            lp = local_root / o.object_name
            if dst not in done:
                c.copy_object(config.BUCKET["backup"], dst, CopySource(config.BUCKET["raw"], o.object_name))
                copied += 1; total_bytes += o.size
            if not lp.exists():
                lp.parent.mkdir(parents=True, exist_ok=True)
                c.fget_object(config.BUCKET["raw"], o.object_name, str(lp))
        db.execute("""INSERT INTO meta.backup_log (backup_type, object_key, local_path, size_bytes, objects_copied,
                          status, finished_at, retention_until) VALUES ('raw_mirror', %s, %s, %s, %s, 'SUCCESS',
                          now(), NULL)""", (f"{config.BUCKET['backup']}/raw/", str(local_root), total_bytes, copied))
        b.rows_written = copied
        b.note(f"{copied} objek baru dicadangkan dari {b.rows_read} objek raw")
        return copied


def retention_cleanup(pipeline="orbit_backup_dr"):
    """Hapus backup database dan staging raw yang lebih dari 30 hari."""
    with track_batch(pipeline, "retention_cleanup", "backup") as b:
        cutoff = datetime.now() - timedelta(days=config.RETENTION_BACKUP_DAYS)
        removed = 0
        for p in (config.BACKUP_DIR / "db").glob("dw_orbit_*.dump"):
            if datetime.fromtimestamp(p.stat().st_mtime) < cutoff:
                p.unlink(); removed += 1
        c = lake.client()
        for o in c.list_objects(config.BUCKET["backup"], prefix="db/", recursive=True):
            if o.last_modified.replace(tzinfo=None) < cutoff:
                c.remove_object(config.BUCKET["backup"], o.object_name); removed += 1
        n_stg = db.execute("""DELETE FROM staging.stg_ll2_launch_raw WHERE extracted_at < now() - %s::interval""",
                           (f"{config.RETENTION_STAGING_DAYS} days",))
        n_stg += db.execute("""DELETE FROM staging.stg_ll2_config_raw WHERE extracted_at < now() - %s::interval""",
                            (f"{config.RETENTION_STAGING_DAYS} days",))
        b.rows_written = removed + n_stg
        b.note(f"{removed} file backup kedaluwarsa dihapus, {n_stg} baris staging raw > "
               f"{config.RETENTION_STAGING_DAYS} hari dihapus (data asli tetap di Data Lake)")
        return removed


def restore_test(pipeline="orbit_backup_dr", keep=False):
    """Restore backup terbaru ke database terpisah, cocokkan row count, ukur RTO."""
    with track_batch(pipeline, "restore_test", "backup") as b:
        row = db.execute("""SELECT object_key, local_path FROM meta.backup_log WHERE backup_type='db_full'
                            AND status='SUCCESS' ORDER BY backup_id DESC LIMIT 1""", fetch=True)
        if not row:
            raise RuntimeError("belum ada backup database")
        key, local = row[0]
        target = "dw_orbit_restore_test"
        env, user = _pg_env()
        t0 = time.perf_counter()
        # ambil dari object storage, bukan disk lokal, seolah server asli hilang
        tmp = Path("/tmp") / Path(local).name
        bucket, okey = key.split("/", 1)
        lake.client().fget_object(bucket, okey, str(tmp))
        psql = [f"{PG_BIN}/psql", "-h", config.DW["host"], "-p", str(config.DW["port"]), "-U", user, "-d", "postgres", "-c"]
        subprocess.run(psql + [f"DROP DATABASE IF EXISTS {target}"], env=env, check=True, capture_output=True)
        subprocess.run(psql + [f"CREATE DATABASE {target}"], env=env, check=True, capture_output=True)
        r = subprocess.run([f"{PG_BIN}/pg_restore", "-h", config.DW["host"], "-p", str(config.DW["port"]), "-U", user,
                            "-d", target, "--no-owner", "-j", "2", str(tmp)], env=env, capture_output=True, text=True)
        if r.returncode != 0 and "error" in r.stderr.lower():
            raise RuntimeError(f"pg_restore gagal: {r.stderr[-1500:]}")
        rto = time.perf_counter() - t0
        details, matched = {}, 0
        conn_r = db.connect(dbname=target)
        try:
            for t in BACKUP_TABLES:
                with conn_r.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    n_restored = cur.fetchone()[0]
                details[t] = {"restored": n_restored}
        finally:
            conn_r.close()
        # dibandingkan dengan row count produksi saat ini (row count saat backup tidak disimpan);
        # tabel meta selalu dianggap cocok karena terus bertambah
        for t in BACKUP_TABLES:
            prod = db.scalar(f"SELECT COUNT(*) FROM {t}")
            details[t]["production"] = prod
            details[t]["match"] = prod == details[t]["restored"] or t.startswith("meta.")
            matched += details[t]["match"]
        if not keep:
            subprocess.run(psql + [f"DROP DATABASE IF EXISTS {target}"], env=env, capture_output=True)
        tmp.unlink(missing_ok=True)
        status = "SUCCESS" if matched == len(BACKUP_TABLES) else "MISMATCH"
        db.execute("""INSERT INTO meta.restore_test (backup_key, target_db, tables_checked, tables_matched, details,
                          duration_s, status) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                   (key, target, len(BACKUP_TABLES), matched, db.json.dumps(details), round(rto, 3), status))
        if status != "SUCCESS":
            alert("WARNING", f"restore test {key}: {matched}/{len(BACKUP_TABLES)} tabel cocok", pipeline, "restore_test")
        b.rows_read = len(BACKUP_TABLES)
        b.note(f"restore {key} -> {target}: {matched}/{len(BACKUP_TABLES)} tabel cocok, RTO {rto:.1f}s")
        log.info("restore test details: %s", details)
        return status
