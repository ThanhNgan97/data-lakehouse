"""Small, idempotent schema upgrades for legacy local databases.

The project does not use Alembic yet.  ``MetaData.create_all`` creates missing
tables, but it deliberately does not alter tables that already exist, so all
backward-compatible ALTER statements live here and are shared by every
database bootstrap entry point.
"""

from sqlalchemy import text


ADDITIVE_MIGRATIONS = (
    "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS dag_run_id VARCHAR(255)",
    (
        "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS "
        "pipeline_status VARCHAR(50) DEFAULT 'pending'"
    ),
    "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS dashboard_slug VARCHAR(255)",
    "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS dashboard_title VARCHAR(512)",
    "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS dashboard_uuid VARCHAR(64)",
    "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS dashboard_status VARCHAR(50) DEFAULT 'pending'",
    "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id)",
    "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS scan_id VARCHAR(36)",
    "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS resource_type VARCHAR(30)",
    "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255)",
    "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ",
    (
        "UPDATE import_jobs SET user_id = created_by "
        "WHERE user_id IS NULL AND created_by IS NOT NULL"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_import_job_idempotency "
        "ON import_jobs (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL"
    ),
    "CREATE INDEX IF NOT EXISTS ix_import_jobs_user_id ON import_jobs (user_id)",
    "CREATE INDEX IF NOT EXISTS ix_import_jobs_scan_id ON import_jobs (scan_id)",
    # URL imports intentionally skip checksumming when deduplication is disabled.
    # Legacy source_files tables made this column mandatory.
    "ALTER TABLE source_files ALTER COLUMN checksum_sha256 DROP NOT NULL",
    "ALTER TABLE api_sources ADD COLUMN IF NOT EXISTS metadata_info JSONB",
    "ALTER TABLE api_sources ADD COLUMN IF NOT EXISTS auth_config JSONB",
    "ALTER TABLE api_sources ADD COLUMN IF NOT EXISTS credential_ciphertext TEXT",
)


def apply_additive_migrations(engine) -> None:
    """Apply safe schema upgrades after ORM metadata has created missing tables."""
    with engine.begin() as connection:
        for statement in ADDITIVE_MIGRATIONS:
            connection.execute(text(statement))
