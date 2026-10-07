"""Idempotent additive migration for local and demo PostgreSQL databases."""

import os
import sys

from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import models  # noqa: F401 - register ORM metadata
from db.database import Base, engine


def migrate() -> None:
    # The current ORM types intentionally remain compatible with legacy tables.
    Base.metadata.create_all(bind=engine)
    with engine.begin() as connection:
        connection.execute(text(
            "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS dag_run_id VARCHAR(255)"
        ))
        connection.execute(text(
            "ALTER TABLE upload_history ADD COLUMN IF NOT EXISTS "
            "pipeline_status VARCHAR(50) DEFAULT 'pending'"
        ))
        # Upgrade the legacy integer-key import_jobs table without deleting data.
        connection.execute(text(
            "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id)"
        ))
        connection.execute(text(
            "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS scan_id VARCHAR(36)"
        ))
        connection.execute(text(
            "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS resource_type VARCHAR(30)"
        ))
        connection.execute(text(
            "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255)"
        ))
        connection.execute(text(
            "ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ"
        ))
        connection.execute(text(
            "UPDATE import_jobs SET user_id = created_by WHERE user_id IS NULL AND created_by IS NOT NULL"
        ))
        connection.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_import_job_idempotency "
            "ON import_jobs (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL"
        ))
    # Creates new manifest, provenance, source identity and outbox tables.
    Base.metadata.create_all(bind=engine)
    print("Database migration completed.")


if __name__ == "__main__":
    migrate()
