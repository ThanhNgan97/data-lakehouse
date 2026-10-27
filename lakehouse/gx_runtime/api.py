"""Small HTTP boundary used by Airflow; GX remains outside Airflow workers."""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from sqlalchemy import create_engine

from .contracts import ContractRegistry
from .engines import SqlAlchemyTableEngine, VersionedSqlSuiteExecutor
from .errors import (
    MetadataExportError,
    QualityGateRejectedError,
    SnapshotMismatchError,
    ValidationConfigurationError,
    ValidationExecutionError,
    ValidationPersistenceError,
)
from .models import ExecutionEngine, ExecutionMode, SourceKind
from .metadata_export import (
    MetadataExportOutcome,
    export_validation_metadata,
    openmetadata_exporter_from_env,
)
from .service import validate_table
from .snapshot_gate import NessieSnapshotResolver, SnapshotAwareQualityGate
from .storage import result_store_from_env
from .suite_registry import SuiteRegistry
from .validation_registry import DatasetValidationRegistry


class CapturingStore:
    def __init__(self, delegate) -> None:
        self.delegate = delegate
        self.uri: str | None = None

    def save(self, summary):
        self.uri = self.delegate.save(summary)
        return self.uri


def _trino_url() -> str:
    return (
        f"trino://{os.getenv('TRINO_USER', 'gx-validator')}@"
        f"{os.getenv('TRINO_HOST', 'trino')}:"
        f"{os.getenv('TRINO_PORT', '8080')}/"
        f"{os.getenv('TRINO_CATALOG', 'lakehouse')}/"
        f"{os.getenv('TRINO_SCHEMA', 'default')}"
    )


class TableValidationApplication:
    def __init__(self) -> None:
        root = os.getenv("GX_CONFIG_ROOT", "/opt/gx/config")
        self.suites = SuiteRegistry(os.path.join(root, "suite_registry.json"))
        self.routes = DatasetValidationRegistry(
            os.path.join(root, "validation_registry.json"),
            contract_registry=ContractRegistry(
                os.path.join(root, "contract_registry.json")
            ),
            suite_registry=self.suites,
        )
        self.sqlalchemy_engine = create_engine(_trino_url(), pool_pre_ping=True)
        self.validator = SqlAlchemyTableEngine(
            VersionedSqlSuiteExecutor(self.suites, self.sqlalchemy_engine)
        )
        self.resolver = NessieSnapshotResolver(
            os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1")
        )
        self.gate = SnapshotAwareQualityGate()
        self.metadata_exporter = openmetadata_exporter_from_env()
        self.metadata_required = (
            os.getenv("GX_OPENMETADATA_REQUIRED", "false").lower() == "true"
        )

    def validate(self, body: dict[str, Any]) -> dict[str, Any]:
        required = {
            "dataset_id", "data_layer", "contract_version", "source_kind",
            "run_id", "nessie_branch", "snapshot_id", "nessie_commit_hash",
        }
        missing = sorted(required - body.keys())
        if missing:
            raise ValidationConfigurationError(
                "Validation request is missing: " + ", ".join(missing)
            )
        try:
            engine = ExecutionEngine(body.get("execution_engine", "sql"))
            source_kind = SourceKind(body["source_kind"])
            execution_mode = ExecutionMode(body.get("execution_mode", "enforce"))
        except ValueError as exc:
            raise ValidationConfigurationError(
                f"Invalid validation enum: {exc}"
            ) from exc
        if engine is not ExecutionEngine.SQL:
            raise ValidationConfigurationError(
                "HTTP table validation currently supports the SQL engine only"
            )
        requested_branch = str(body["nessie_branch"])
        catalog_branch = os.getenv("GX_TRINO_NESSIE_BRANCH", "main")
        if requested_branch != catalog_branch:
            raise ValidationConfigurationError(
                "Requested branch is not mounted by this Trino-backed GX service; "
                "deploy it with GX_TRINO_NESSIE_BRANCH matching the candidate"
            )
        plan = self.routes.resolve(
            dataset_id=str(body["dataset_id"]),
            data_layer=str(body["data_layer"]),
            contract_version=str(body["contract_version"]),
            execution_engine=engine,
            source_kind=source_kind,
        )
        current = self.resolver.resolve(plan.data_asset_ref, requested_branch)
        requested_identity = (
            str(body["snapshot_id"]),
            str(body["nessie_commit_hash"]),
        )
        if requested_identity != (current.snapshot_id, current.nessie_commit_hash):
            error = SnapshotMismatchError(
                "SNAPSHOT_MISMATCH; refresh identity and run GX again"
            )
            error.current_identity = current
            raise error
        store = CapturingStore(result_store_from_env())
        summary = validate_table(
            data_asset_ref=plan.data_asset_ref,
            dataset_id=plan.dataset_id,
            data_layer=plan.data_layer,
            contract_version=plan.contract_version,
            suite_names=tuple(suite.name for suite in plan.suites),
            suite_versions={suite.name: suite.version for suite in plan.suites},
            run_id=str(body["run_id"]),
            nessie_branch=current.nessie_branch,
            snapshot_id=current.snapshot_id,
            nessie_commit_hash=current.nessie_commit_hash,
            validator=self.validator,
            result_store=store,
            execution_mode=execution_mode,
            source_kind=plan.source_kind,
        )
        approval = self.gate.verify_current(summary, self.resolver)
        if not summary.merge_allowed:
            raise QualityGateRejectedError("DATA_QUALITY_FAILED")
        metadata = MetadataExportOutcome(False, 0, 0)
        if self.metadata_exporter is not None:
            metadata = export_validation_metadata(
                summary,
                str(store.uri),
                self.metadata_exporter,
                required=self.metadata_required,
            )
        return {
            "status": summary.status.value,
            "validation_id": summary.validation_id,
            "validation_uri": store.uri,
            "merge_allowed": summary.merge_allowed,
            "approval": approval.as_dict(),
            "metadata_export": {
                "enabled": metadata.enabled,
                "published": metadata.published,
                "failed": metadata.failed,
            },
        }


ERRORS = (
    (SnapshotMismatchError, "SNAPSHOT_MISMATCH", 409, True),
    (ValidationConfigurationError, "GX_CONFIGURATION_ERROR", 400, False),
    (ValidationPersistenceError, "GX_EXECUTION_ERROR", 503, True),
    (ValidationExecutionError, "GX_EXECUTION_ERROR", 503, True),
    (MetadataExportError, "GX_EXECUTION_ERROR", 503, True),
    (QualityGateRejectedError, "DATA_QUALITY_FAILED", 422, False),
)


class Handler(BaseHTTPRequestHandler):
    application: TableValidationApplication

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        if self.path == "/health/ready":
            self._send(200, {"status": "ready"})
        else:
            self._send(404, {"error_code": "NOT_FOUND"})

    def do_POST(self) -> None:
        if self.path != "/v1/validate/table":
            self._send(404, {"error_code": "NOT_FOUND"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length))
            self._send(200, self.application.validate(body))
        except Exception as exc:
            for error_type, code, status, retryable in ERRORS:
                if isinstance(exc, error_type):
                    payload = {
                        "error_code": code,
                        "message": str(exc),
                        "retryable": retryable,
                    }
                    current = getattr(exc, "current_identity", None)
                    if current:
                        payload["current_identity"] = {
                            "nessie_branch": current.nessie_branch,
                            "snapshot_id": current.snapshot_id,
                            "nessie_commit_hash": current.nessie_commit_hash,
                        }
                    self._send(status, payload)
                    return
            self._send(
                500,
                {"error_code": "GX_EXECUTION_ERROR", "retryable": True},
            )

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def main() -> None:
    Handler.application = TableValidationApplication()
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("GX_HTTP_PORT", "8090"))), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
