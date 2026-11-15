"""Data Lake (MinIO, S3-compatible).

Zona:
  orbit-raw     (Bronze) : data asli dari sumber (CSV apa adanya, JSON API per halaman .json.gz)
  orbit-clean   (Silver) : hasil cleaning/standardisasi (Parquet)
  orbit-curated (Gold)   : data mart & artefak model (Parquet / joblib)
  orbit-backup           : pg_dump + mirror raw (disaster recovery)

Pola key (partisi gaya Hive):
  source=<src>/dataset=<ds>/ingest_date=YYYY-MM-DD/batch=<id>/<file>
"""
import gzip
import hashlib
import io
import json
from datetime import date

from minio import Minio
from minio.error import S3Error

from . import config, db

_client = None


def client() -> Minio:
    global _client
    if _client is None:
        _client = Minio(**config.MINIO)
    return _client


def raw_key(source, dataset, ingest_date: date, batch_id, filename):
    return (f"source={source}/dataset={dataset}/ingest_date={ingest_date:%Y-%m-%d}/"
            f"batch={batch_id}/{filename}")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def put_bytes(zone, key, data: bytes, content_type="application/octet-stream", *, source=None,
              dataset=None, batch_id=None, ingest_date=None, record_count=None, register=True):
    """Upload objek lalu catat di meta.lake_object."""
    bucket = config.BUCKET[zone]
    digest = sha256(data)
    client().put_object(bucket, key, io.BytesIO(data), len(data), content_type=content_type,
                        metadata={"sha256": digest, "batch-id": str(batch_id or "")})
    if register:
        db.execute(
            """INSERT INTO meta.lake_object (object_key, bucket, zone, source, dataset, batch_id,
                   ingest_date, record_count, size_bytes, sha256, content_type)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (object_key) DO UPDATE SET size_bytes=EXCLUDED.size_bytes,
                   sha256=EXCLUDED.sha256, record_count=EXCLUDED.record_count, created_at=now()""",
            (f"{bucket}/{key}", bucket, zone, source, dataset, batch_id, ingest_date,
             record_count, len(data), digest, content_type))
    return f"{bucket}/{key}", digest


def put_json_gz(zone, key, obj, **kw):
    data = gzip.compress(json.dumps(obj, ensure_ascii=False).encode("utf-8"), compresslevel=6)
    return put_bytes(zone, key, data, "application/gzip", **kw)


def put_parquet(zone, key, df, **kw):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return put_bytes(zone, key, buf.getvalue(), "application/vnd.apache.parquet",
                     record_count=len(df), **kw)


def get_bytes(full_key: str) -> bytes:
    bucket, key = full_key.split("/", 1)
    resp = client().get_object(bucket, key)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def get_json_gz(full_key: str):
    return json.loads(gzip.decompress(get_bytes(full_key)).decode("utf-8"))


def exists(full_key: str) -> bool:
    bucket, key = full_key.split("/", 1)
    try:
        client().stat_object(bucket, key)
        return True
    except S3Error:
        return False


def list_objects(zone, prefix=""):
    return list(client().list_objects(config.BUCKET[zone], prefix=prefix, recursive=True))
