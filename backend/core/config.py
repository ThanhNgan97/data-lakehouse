import os
from pathlib import Path

# Load backend/.env first, then project-root .env as fallback
try:
    from dotenv import load_dotenv
    _backend_env_path = Path(__file__).resolve().parent.parent / ".env"
    _root_env_path = Path(__file__).resolve().parent.parent.parent / ".env"

    if _backend_env_path.exists():
        load_dotenv(_backend_env_path, override=False)

    if _root_env_path.exists():
        load_dotenv(_root_env_path, override=False)
except ImportError:
    pass  # python-dotenv chưa cài — biến hệ thống vẫn được đọc qua os.getenv

SECRET_KEY = os.getenv("SECRET_KEY", "nhuquynh_data_lakehouse_secret_key")
API_SOURCE_CREDENTIAL_KEY = os.getenv("API_SOURCE_CREDENTIAL_KEY", "")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 12

# S3-compatible storage (Apache Ozone S3 Gateway)
S3_ENDPOINT    = os.getenv("S3_ENDPOINT", "http://127.0.0.1:9878")
S3_ACCESS_KEY  = os.getenv("S3_ACCESS_KEY", "lakehouse")
S3_SECRET_KEY  = os.getenv("S3_SECRET_KEY", "lakehouse-secret")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "university-lakehouse")
S3_REGION      = os.getenv("S3_REGION", "us-east-1")
MAX_UPLOAD_FILE_BYTES = int(os.getenv("MAX_UPLOAD_FILE_BYTES", str(100 * 1024 * 1024)))

# Nessie
NESSIE_API_URL = os.getenv("NESSIE_API_URL", "http://localhost:19120/api/v1")

# Airflow
AIRFLOW_WEBSERVER_URL = os.getenv("AIRFLOW_WEBSERVER_URL", "http://localhost:8080")
AIRFLOW_FILE_DAG_ID = os.getenv("AIRFLOW_FILE_DAG_ID", "universal_lakehouse_pipeline")
AIRFLOW_API_DAG_ID = os.getenv("AIRFLOW_API_DAG_ID", "api_dataset_pipeline")
AIRFLOW_API_USERNAME = os.getenv("AIRFLOW_API_USERNAME", "airflow")
AIRFLOW_API_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "")

# CTU IOC demo source used by the backend preview endpoint.
MOCK_API_PUBLIC_URL = os.getenv("MOCK_API_PUBLIC_URL", "http://localhost:8090")
MOCK_API_INTERNAL_URL = os.getenv("MOCK_API_INTERNAL_URL", "http://ctu-ioc-mock-api:8000")
MOCK_API_KEY = os.getenv("MOCK_API_KEY", "")

# Universal URL Ingestion rollout flags.
URL_INGESTION_ENABLED = os.getenv("URL_INGESTION_ENABLED", "true").lower() == "true"
URL_CHECKSUM_DEDUP_ENABLED = os.getenv("URL_CHECKSUM_DEDUP_ENABLED", "false").lower() == "true"
GOOGLE_DRIVE_INGESTION_ENABLED = os.getenv("GOOGLE_DRIVE_INGESTION_ENABLED", "true").lower() == "true"

