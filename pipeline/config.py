"""Konfigurasi pipeline. Credential dibaca dari environment (.env)."""
import os
from pathlib import Path

PROJECT_DIR = Path(os.getenv("ORBIT_PROJECT_DIR", Path(__file__).resolve().parents[1]))
DATA_DIR = PROJECT_DIR / "data"
SOURCE_DIR = DATA_DIR / "source"          # folder tempat file arsip internal diletakkan
MAPPING_DIR = PROJECT_DIR / "mappings"
BACKUP_DIR = PROJECT_DIR / "backups"
DDL_DIR = PROJECT_DIR / "sql" / "ddl"

DW = dict(
    host=os.getenv("DW_HOST", "localhost"),
    port=int(os.getenv("DW_PORT", "5433")),
    dbname=os.getenv("DW_DB", "dw_orbit"),
    user=os.getenv("DW_ETL_USER", "etl_writer"),
    password=os.getenv("DW_ETL_PASSWORD", ""),
)

MINIO = dict(
    endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_PIPELINE_USER", ""),
    secret_key=os.getenv("MINIO_PIPELINE_PASSWORD", ""),
    secure=False,
)
BUCKET = {"raw": "orbit-raw", "clean": "orbit-clean", "curated": "orbit-curated", "backup": "orbit-backup"}

LL2_BASE_URL = os.getenv("LL2_BASE_URL", "https://ll.thespacedevs.com/2.3.0").rstrip("/")
LL2_RATE_LIMIT = int(os.getenv("LL2_RATE_LIMIT_PER_HOUR", "15"))
LL2_API_TOKEN = os.getenv("LL2_API_TOKEN", "")
LL2_PAGE_LIMIT = 100
# peluncuran terakhir di arsip internal (TI-01): LL2 hanya diambil setelah titik ini
ARCHIVE_CUTOFF_UTC = "2022-07-29T13:28:00Z"

ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

# TV-08: load sumber dibatalkan bila reject > 5%
QUALITY_GATE_REJECT_PCT = 5.0
# retensi (hari)
RETENTION_STAGING_DAYS = 30
RETENTION_RAW_JSON_DAYS = 90
RETENTION_BACKUP_DAYS = 30
