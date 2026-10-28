"""Small HTTP boundary used by Airflow; GX remains outside Airflow workers."""

from __future__ import annotations

import json
import os
import signal
from time import perf_counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any

from sqlalchemy import create_engine

try:
    from gx_validation_request import (
        GXTableValidationRequest,
        GXValidationRequestError,
    )
except ModuleNotFoundError:  # Local repository tests import through lakehouse.
    from spark.gx_validation_request import (
        GXTableValidationRequest,
        GXValidationRequestError,
    )
try:
    from gx_candidate_binding import (
        CandidateBindingError,
        dbt_gold_candidate_binding,
    )
except ModuleNotFoundError:
    from spark.gx_candidate_binding import (
        CandidateBindingError,
        dbt_gold_candidate_binding,
    )

from .contracts import ContractRegistry
from .candidate_engine import CandidateSparkSqlSuiteExecutor
from .candidate_spark import CandidateSparkSessions
from .engines import SqlAlchemyTableEngine, VersionedSqlSuiteExecutor
from .errors import (
    MetadataExportError,
    QualityGateRejectedError,
    SnapshotMismatchError,
    ValidationConfigurationError,
    ValidationExecutionError,
    ValidationPersistenceError,
)
from .models import (
    ContractStatus,
    ExecutionEngine,
    ExecutionMode,
    SourceKind,
    SuiteReference,
    ValidationCoverage,
)
from .validation_registry import ValidationPlan
from .metadata_export import (
    MetadataExportOutcome,
    export_validation_metadata,
    openmetadata_exporter_from_env,
)
from .observability import GXMetrics
from .production import ProductionSettings
from .revocation import SnapshotRevocation, revocation_store_from_env
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
        self.accepting_requests = False
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
        self.candidate_sessions = CandidateSparkSessions()
        self.candidate_validator = SqlAlchemyTableEngine(
            CandidateSparkSqlSuiteExecutor(
                self.suites,
                self.candidate_sessions.for_branch,
            )
        )
        self.resolver = NessieSnapshotResolver(
            os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1")
        )
        self.gate = SnapshotAwareQualityGate()
        self.metadata_exporter = openmetadata_exporter_from_env()
        self.metadata_required = (
            os.getenv("GX_OPENMETADATA_REQUIRED", "false").lower() == "true"
        )
        self.metrics = GXMetrics()
        self.production = ProductionSettings.from_env()
        self.revocations = revocation_store_from_env()
        self.accepting_requests = True

    def close(self) -> None:
        """Stop admitting work and release pooled client connections."""
        self.accepting_requests = False
        self.sqlalchemy_engine.dispose()
        self.candidate_sessions.close()

    def _validator_for_branch(self, requested_branch: str):
        catalog_branch = os.getenv("GX_TRINO_NESSIE_BRANCH", "main")
        if requested_branch == catalog_branch:
            return self.validator
        if os.getenv("GX_CANDIDATE_SPARK_ENABLED", "true").lower() != "true":
            raise ValidationConfigurationError(
                "Candidate branch validation is disabled in this GX runtime"
            )
        return self.candidate_validator

    def validate(self, body: dict[str, Any]) -> dict[str, Any]:
        started = perf_counter()
        layer = str(body.get("data_layer", "unknown"))
        engine = str(body.get("execution_engine", "sql"))
        status = "ERROR"
        summary = None
        metrics = getattr(self, "metrics", None)
        if metrics is None:
            metrics = self.metrics = GXMetrics()
        try:
            retry_attempt = int(body.get("retry_attempt", 0))
        except (TypeError, ValueError) as exc:
            raise ValidationConfigurationError("retry_attempt must be an integer") from exc
        if retry_attempt > 0:
            metrics.record_retry(engine)
        try:
            response, summary = self._validate(body)
            status = summary.status.value
            return response
        except QualityGateRejectedError:
            status = "FAILED"
            metrics.record_quality_failure(layer, engine)
            raise
        except TimeoutError:
            status = "TIMEOUT"
            metrics.record_timeout(engine)
            raise
        except (ValidationExecutionError, ValidationPersistenceError, MetadataExportError) as exc:
            if _caused_by_timeout(exc):
                status = "TIMEOUT"
                metrics.record_timeout(engine)
            else:
                metrics.record_engine_error(engine)
            raise
        finally:
            values = summary.metrics if summary is not None else {}
            metrics.observe_validation(
                duration_seconds=perf_counter() - started,
                layer=layer,
                engine=engine,
                status=status,
                rows=_metric_value(values, "validation_rows", "row_count", "dataset_rows"),
                bytes_scanned=_metric_value(
                    values,
                    "validation_bytes_scanned",
                    "bytes_scanned",
                    "trino_bytes_scanned",
                ),
            )

    def _validate(self, body: dict[str, Any]) -> tuple[dict[str, Any], Any]:
        try:
            request = GXTableValidationRequest.from_mapping(body)
            engine = ExecutionEngine(request.execution_engine)
            source_kind = SourceKind(request.source_kind)
            execution_mode = ExecutionMode(request.execution_mode)
            coverage = ValidationCoverage(request.coverage)
        except (GXValidationRequestError, ValueError) as exc:
            raise ValidationConfigurationError(
                f"Invalid GX table validation request: {exc}"
            ) from exc
        self.production.validate_mode(execution_mode)
        if engine is not ExecutionEngine.SQL:
            raise ValidationConfigurationError(
                "HTTP table validation currently supports the SQL engine only"
            )
        requested_branch = request.nessie_branch
        validator = self._validator_for_branch(requested_branch)
        if coverage is ValidationCoverage.BASELINE:
            plan = ValidationPlan(
                dataset_id=request.dataset_id,
                data_layer="silver",
                contract_version="baseline-1.0.0",
                data_asset_ref=request.data_asset_ref,
                execution_engine=engine,
                suites=(SuiteReference("lakehouse_baseline_silver", "1.0.0"),),
                source_kind=source_kind,
            )
            contract_status = ContractStatus.MISSING
        else:
            plan = self.routes.resolve(
                dataset_id=request.dataset_id,
                data_layer=request.data_layer,
                contract_version=request.contract_version,
                execution_engine=engine,
                source_kind=source_kind,
            )
            if request.data_asset_ref != plan.data_asset_ref:
                raise ValidationConfigurationError(
                    "GX request asset does not match the canonical route: "
                    f"{request.data_asset_ref!r} != {plan.data_asset_ref!r}"
                )
            contract_status = ContractStatus.APPROVED
        execution_asset_ref = request.execution_asset_ref
        if execution_asset_ref != plan.data_asset_ref:
            try:
                binding = dbt_gold_candidate_binding(
                    request.dataset_id,
                    request.run_id,
                    plan.data_asset_ref,
                )
            except CandidateBindingError as exc:
                raise ValidationConfigurationError(str(exc)) from exc
            if (
                plan.data_layer not in {"gold", "reconciliation"}
                or requested_branch != binding.candidate_branch
                or execution_asset_ref != binding.physical_asset_ref
            ):
                raise ValidationConfigurationError(
                    "Physical GX asset does not match the authorized dbt candidate binding"
                )
        current = self.resolver.resolve(execution_asset_ref, requested_branch)
        requested_identity = (
            request.snapshot_id,
            request.nessie_commit_hash,
        )
        if requested_identity != (current.snapshot_id, current.nessie_commit_hash):
            self.revocations.save(
                SnapshotRevocation(
                    dataset_id=plan.dataset_id,
                    data_layer=plan.data_layer,
                    nessie_branch=requested_branch,
                    revoked_snapshot_id=requested_identity[0],
                    revoked_commit_hash=requested_identity[1],
                    current_snapshot_id=current.snapshot_id,
                    current_commit_hash=current.nessie_commit_hash,
                )
            )
            error = SnapshotMismatchError(
                "SNAPSHOT_MISMATCH; refresh identity and run GX again"
            )
            error.current_identity = current
            raise error
        store = CapturingStore(result_store_from_env())
        summary = validate_table(
            data_asset_ref=execution_asset_ref,
            dataset_id=plan.dataset_id,
            data_layer=plan.data_layer,
            contract_version=plan.contract_version,
            suite_names=tuple(suite.name for suite in plan.suites),
            suite_versions={suite.name: suite.version for suite in plan.suites},
            run_id=request.run_id,
            nessie_branch=current.nessie_branch,
            snapshot_id=current.snapshot_id,
            nessie_commit_hash=current.nessie_commit_hash,
            validator=validator,
            result_store=store,
            execution_mode=execution_mode,
            source_kind=plan.source_kind,
            suite_revision=self.production.suite_revision,
            coverage=coverage,
            contract_status=contract_status,
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
            "coverage": summary.coverage.value,
            "contract_status": summary.contract_status.value,
            "approval": approval.as_dict(),
            "metadata_export": {
                "enabled": metadata.enabled,
                "published": metadata.published,
                "failed": metadata.failed,
            },
            "suite_revision": summary.suite_revision,
        }, summary


def _metric_value(values: dict[str, Any], *names: str) -> float | None:
    for name in names:
        value = values.get(name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None


def _caused_by_timeout(error: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, TimeoutError):
            return True
        current = current.__cause__ or current.__context__
    return False


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
        if self.path == "/health/live":
            self._send(200, {"status": "alive"})
        elif self.path == "/health/ready":
            if self.application.accepting_requests:
                self._send(200, {"status": "ready"})
            else:
                self._send(503, {"status": "not_ready"})
        elif self.path == os.getenv("GX_METRICS_PATH", "/metrics"):
            raw = self.application.metrics.render().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        else:
            self._send(404, {"error_code": "NOT_FOUND"})

    def do_POST(self) -> None:
        if self.path != "/v1/validate/table":
            self._send(404, {"error_code": "NOT_FOUND"})
            return
        if not self.application.accepting_requests:
            self._send(
                503,
                {"error_code": "GX_UNAVAILABLE", "retryable": True},
            )
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
    server.daemon_threads = False

    def begin_shutdown(_signum=None, _frame=None) -> None:
        Handler.application.accepting_requests = False
        # BaseServer.shutdown must run from a different thread than serve_forever.
        Thread(target=server.shutdown, name="gx-http-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, begin_shutdown)
    signal.signal(signal.SIGINT, begin_shutdown)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        # server_close waits for non-daemon request threads before pooled
        # dependencies are disposed.
        server.server_close()
        Handler.application.close()


if __name__ == "__main__":
    main()
