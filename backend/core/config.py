import os
from pathlib import Path

# Load file .env tại thư mục gốc dự án (2 cấp trên backend/)
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=False)
except ImportError:
    pass  # python-dotenv chưa cài — biến hệ thống vẫn được đọc qua os.getenv

SECRET_KEY = os.getenv("SECRET_KEY", "nhuquynh_data_lakehouse_secret_key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 12

# MinIO Config
MINIO_URL         = os.getenv("MINIO_URL",         "127.0.0.1:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "university-lakehouse")

# Nessie
NESSIE_API_URL = os.getenv("NESSIE_API_URL", "http://localhost:19120/api/v1")

# Airflow
AIRFLOW_WEBSERVER_URL = os.getenv("AIRFLOW_WEBSERVER_URL", "http://localhost:8080")

