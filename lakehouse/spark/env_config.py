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
            load_dotenv(_env_path, override=True)
            break
except ImportError:
    pass  # Không có dotenv vẫn OK nếu biến đã set qua os.environ / setx


IS_DOCKER = os.path.exists("/.dockerenv")

# ── MinIO ────────────────────────────────────────────────────
_minio_ep = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
if IS_DOCKER and ("127.0.0.1" in _minio_ep or "localhost" in _minio_ep):
    MINIO_ENDPOINT = _minio_ep.replace("127.0.0.1", "minio").replace("localhost", "minio")
else:
    MINIO_ENDPOINT = _minio_ep

MINIO_ACCESS_KEY  = os.environ.get("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY  = os.environ.get("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME", "university-lakehouse")

# ── PostgreSQL ───────────────────────────────────────────────
_pg_host = os.environ.get("PG_HOST", "127.0.0.1")
if IS_DOCKER and (_pg_host in ["127.0.0.1", "localhost"]):
    PG_HOST = "postgres"
else:
    PG_HOST = _pg_host

_pg_port = int(os.environ.get("PG_PORT", "5432"))
if IS_DOCKER and PG_HOST == "postgres" and _pg_port == 5433:
    PG_PORT = 5432
else:
    PG_PORT = _pg_port
PG_USER           = os.environ.get("PG_USER",           "postgres")
PG_PASSWORD       = os.environ.get("PG_PASSWORD",       "240203")
PG_MAINTENANCE_DB = os.environ.get("PG_MAINTENANCE_DB", "postgres")
NESSIE_DB         = os.environ.get("NESSIE_DB",         "nessie_db")


# ── MySQL OLTP (nguồn KPI có cấu trúc) ──────────────────────
_mysql_host = os.environ.get("MYSQL_HOST", "127.0.0.1")
if IS_DOCKER and (_mysql_host in ["127.0.0.1", "localhost"]):
    MYSQL_HOST = "mysql"
else:
    MYSQL_HOST = _mysql_host

MYSQL_PORT     = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "cusc_kpi_operational")
MYSQL_USER     = os.environ.get("MYSQL_USER", "kpi_user")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")

MYSQL_JDBC_JAR = os.environ.get(
    "MYSQL_JDBC_JAR",
    "/opt/airflow/jars/mysql-connector-j-8.2.0.jar"
    if IS_DOCKER
    else str(_ROOT_DIR / "jars" / "mysql-connector-j-8.2.0.jar"),
)

# ── Nessie ───────────────────────────────────────────────────
_nessie_url = os.environ.get("NESSIE_API_URL", "http://localhost:19120/api/v1")
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
HADOOP_HOME       = os.environ.get("HADOOP_HOME",       "/opt/hadoop" if IS_DOCKER else r"C:\hadoop")
SPARK_LOCAL_IP    = os.environ.get("SPARK_LOCAL_IP",    "127.0.0.1")

# ── Google Gemini API ─────────────────────────────────────────
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY",    "")
