# -*- coding: utf-8 -*-


import os
from pathlib import Path

# Tìm file .env tại thư mục gốc dự án (2 cấp trên thư mục spark/)
_ROOT_DIR = Path(__file__).resolve().parent.parent.parent  # lakehouse/spark -> lakehouse -> root
_ENV_FILE_CANDIDATES = [
    _ROOT_DIR / ".env",
    Path("/opt/airflow/.env"),
    Path("/opt/airflow/spark/.env"),
    Path(__file__).resolve().parent / ".env",
]

# Load .env nếu python-dotenv có sẵn (optional dependency)
try:
    # pyrefly: ignore [missing-import]
    from dotenv import load_dotenv
    for _env_path in _ENV_FILE_CANDIDATES:
        if _env_path.exists():
            # Runtime/container variables are authoritative. The local .env
            # file should only provide values that are not already configured.
            load_dotenv(_env_path, override=False)
            break
except ImportError:
    pass  # Không có dotenv vẫn OK nếu biến đã set qua os.environ / setx


IS_DOCKER = os.path.exists("/.dockerenv")

# ── Apache Ozone S3 Gateway ─────────────────────────────────
_s3_ep = os.environ.get("S3_ENDPOINT") or "http://127.0.0.1:9878"
if IS_DOCKER and ("127.0.0.1" in _s3_ep or "localhost" in _s3_ep):
    S3_ENDPOINT = _s3_ep.replace("127.0.0.1", "s3g").replace("localhost", "s3g")
else:
    S3_ENDPOINT = _s3_ep

S3_ACCESS_KEY  = os.environ.get("S3_ACCESS_KEY", "lakehouse")
S3_SECRET_KEY  = os.environ.get("S3_SECRET_KEY", "lakehouse-secret")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME") or "university-lakehouse"

# Compatibility aliases while the Spark jobs are migrated to storage-neutral names.
MINIO_ENDPOINT = S3_ENDPOINT
MINIO_ACCESS_KEY = S3_ACCESS_KEY
MINIO_SECRET_KEY = S3_SECRET_KEY
MINIO_BUCKET_NAME = S3_BUCKET_NAME

# ── PostgreSQL ───────────────────────────────────────────────
_pg_host = os.environ.get("PG_HOST") or "127.0.0.1"
_pg_port = os.environ.get("PG_PORT") or "5432"

if IS_DOCKER and (_pg_host in ["127.0.0.1", "localhost", "postgres"]):
    PG_HOST = "postgres"
    PG_PORT = 5432
else:
    PG_HOST = _pg_host
    PG_PORT = int(_pg_port)

PG_USER           = os.environ.get("PG_USER") or "postgres"
PG_PASSWORD       = os.environ.get("PG_PASSWORD",       "240203")
PG_DATABASE       = os.environ.get("PG_DATABASE") or "university_db"
PG_MAINTENANCE_DB = os.environ.get("PG_MAINTENANCE_DB") or "postgres"
NESSIE_DB         = os.environ.get("NESSIE_DB") or "nessie_db"

# ── Nessie ───────────────────────────────────────────────────
_nessie_url = os.environ.get("NESSIE_API_URL") or "http://localhost:19120/api/v1"
if IS_DOCKER and ("127.0.0.1" in _nessie_url or "localhost" in _nessie_url):
    NESSIE_API_URL = _nessie_url.replace("127.0.0.1", "nessie").replace("localhost", "nessie")
else:
    NESSIE_API_URL = _nessie_url

# ── OpenMetadata ─────────────────────────────────────────────
_om_host = os.environ.get("OPENMETADATA_HOST_PORT", "http://localhost:8585/api")
if IS_DOCKER and ("127.0.0.1" in _om_host or "localhost" in _om_host):
    OPENMETADATA_HOST_PORT = _om_host.replace("127.0.0.1", "openmetadata_server").replace("localhost", "openmetadata_server")
else:
    OPENMETADATA_HOST_PORT = _om_host

OPENMETADATA_JWT_TOKEN  = os.environ.get("OPENMETADATA_JWT_TOKEN",  "")

# ── Spark / Hadoop (Windows vs Linux Docker) ─────────────────
HADOOP_HOME       = os.environ.get("HADOOP_HOME") or ("/opt/hadoop" if IS_DOCKER else r"C:\hadoop")
SPARK_LOCAL_IP    = os.environ.get("SPARK_LOCAL_IP") or "127.0.0.1"

# ── Google Gemini API ─────────────────────────────────────────
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY",    "")
