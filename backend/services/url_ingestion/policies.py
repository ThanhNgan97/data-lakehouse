import os


MAX_FILE_BYTES = int(os.getenv("MAX_UPLOAD_FILE_BYTES", str(100 * 1024 * 1024)))
MAX_JOB_BYTES = int(os.getenv("URL_IMPORT_MAX_JOB_BYTES", str(1024 * 1024 * 1024)))
MAX_FILES_PER_JOB = int(os.getenv("URL_IMPORT_MAX_FILES", "100"))
# Files targeting the same Iceberg tables must merge serially; concurrent
# Nessie branches can conflict when they touch identical Silver/Gold keys.
MAX_ACTIVE_FILES_PER_JOB = int(os.getenv("URL_IMPORT_MAX_ACTIVE_FILES", "1"))
SCAN_TTL_MINUTES = int(os.getenv("URL_IMPORT_SCAN_TTL_MINUTES", "15"))
MAX_REDIRECTS = int(os.getenv("URL_IMPORT_MAX_REDIRECTS", "5"))
SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".csv", ".tsv", ".xlsx", ".xls", ".json", ".parquet"}
SUPPORTED_MIME_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/csv",
    "application/csv",
    "application/json",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/vnd.apache.parquet",
}
