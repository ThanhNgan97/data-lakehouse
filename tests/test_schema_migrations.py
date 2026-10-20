import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from db.schema_migrations import ADDITIVE_MIGRATIONS, apply_additive_migrations


class _RecordingConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


class _BeginContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _RecordingEngine:
    def __init__(self):
        self.connection = _RecordingConnection()

    def begin(self):
        return _BeginContext(self.connection)


def test_additive_migrations_cover_url_import_legacy_schema():
    sql = "\n".join(ADDITIVE_MIGRATIONS)
    for column in (
        "user_id",
        "scan_id",
        "resource_type",
        "idempotency_key",
        "cancel_requested_at",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
    for column in ("dashboard_slug", "dashboard_title", "dashboard_uuid", "dashboard_status"):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
    assert "checksum_sha256 DROP NOT NULL" in sql


def test_apply_additive_migrations_executes_every_statement():
    engine = _RecordingEngine()
    apply_additive_migrations(engine)
    assert engine.connection.statements == list(ADDITIVE_MIGRATIONS)
