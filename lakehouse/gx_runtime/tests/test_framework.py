from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from gx_runtime.engines import SqlAlchemyTableEngine
from gx_runtime.errors import (
    SuiteNotFoundError,
    ValidationConfigurationError,
    ValidationExecutionError,
    ValidationPersistenceError,
)
from gx_runtime.models import (
    EngineValidationResult,
    ExecutionEngine,
    ExecutionMode,
    ExpectationFailure,
    SuiteReference,
    ValidationRequest,
    ValidationStatus,
)
from gx_runtime.service import validate_dataframe, validate_table
from gx_runtime.storage import InMemoryResultStore, S3ResultStore, result_store_from_env
from gx_runtime.suite_registry import SuiteRegistry


SUITES = (SuiteReference("teaching_progress", "1.0.0"),)


class FakeDataFrameEngine:
    def __init__(self, result: EngineValidationResult) -> None:
        self.result = result
        self.calls = 0

    def validate_dataframe(self, dataframe, request):
        self.calls += 1
        return self.result


class FakeTableEngine:
    def __init__(self, result: EngineValidationResult) -> None:
        self.result = result
        self.request = None

    def validate_table(self, request):
        self.request = request
        return self.result


PASS_RESULT = EngineValidationResult(
    success=True,
    evaluated_expectations=2,
    successful_expectations=2,
    result_ids=("result-1",),
)


def dataframe_kwargs(engine, store, mode=ExecutionMode.ENFORCE):
    return {
        "data_asset_ref": "dataframe://education.teaching_progress/silver",
        "dataset_id": "education.teaching_progress",
        "data_layer": "silver",
        "contract_version": "1.0.0",
        "suite_names": ("teaching_progress",),
        "suite_versions": {"teaching_progress": "1.0.0"},
        "run_id": "run-123",
        "validator": engine,
        "result_store": store,
        "execution_mode": mode,
        "nessie_branch": "candidate_123",
        "snapshot_id": "456",
        "nessie_commit_hash": "abc123",
    }


class ValidationServiceTest(unittest.TestCase):
    def test_enforce_pass_preserves_traceability_metadata(self):
        engine = FakeDataFrameEngine(PASS_RESULT)
        store = InMemoryResultStore()

        summary = validate_dataframe(object(), **dataframe_kwargs(engine, store))

        self.assertEqual(summary.status, ValidationStatus.PASSED)
        self.assertTrue(summary.merge_allowed)
        self.assertTrue(summary.executed)
        self.assertEqual(summary.execution_engine, ExecutionEngine.SPARK)
        self.assertEqual(summary.snapshot_id, "456")
        self.assertEqual(summary.nessie_commit_hash, "abc123")
        self.assertEqual(summary.suite_versions, {"teaching_progress": "1.0.0"})
        self.assertIn(summary.validation_id, store.records)
        json.dumps(summary.as_dict())

    def test_enforce_critical_failure_blocks_merge(self):
        result = EngineValidationResult(
            success=False,
            evaluated_expectations=1,
            successful_expectations=0,
            failures=(ExpectationFailure("not_null", "critical", "id"),),
        )
        summary = validate_dataframe(
            object(), **dataframe_kwargs(FakeDataFrameEngine(result), InMemoryResultStore())
        )

        self.assertEqual(summary.status, ValidationStatus.FAILED)
        self.assertFalse(summary.merge_allowed)
        self.assertEqual(len(summary.critical_failures), 1)

    def test_report_converts_failure_to_warning_and_allows_merge(self):
        result = EngineValidationResult(
            success=False,
            evaluated_expectations=1,
            successful_expectations=0,
            failures=(ExpectationFailure("range", "critical"),),
        )
        summary = validate_dataframe(
            object(),
            **dataframe_kwargs(
                FakeDataFrameEngine(result),
                InMemoryResultStore(),
                ExecutionMode.REPORT,
            ),
        )

        self.assertEqual(summary.status, ValidationStatus.WARNING)
        self.assertTrue(summary.merge_allowed)

    def test_enforce_warning_failure_does_not_block_merge(self):
        result = EngineValidationResult(
            success=False,
            evaluated_expectations=1,
            successful_expectations=0,
            failures=(ExpectationFailure("freshness", "warning"),),
        )
        summary = validate_dataframe(
            object(), **dataframe_kwargs(FakeDataFrameEngine(result), InMemoryResultStore())
        )

        self.assertEqual(summary.status, ValidationStatus.WARNING)
        self.assertTrue(summary.merge_allowed)
        self.assertEqual(len(summary.warning_failures), 1)

    def test_off_does_not_call_engine_but_persists_summary(self):
        engine = FakeDataFrameEngine(PASS_RESULT)
        store = InMemoryResultStore()

        summary = validate_dataframe(
            object(), **dataframe_kwargs(engine, store, ExecutionMode.OFF)
        )

        self.assertEqual(engine.calls, 0)
        self.assertFalse(summary.executed)
        self.assertEqual(summary.evaluated_expectations, 0)
        self.assertIn(summary.validation_id, store.records)

    def test_same_identity_creates_traceable_deterministic_id(self):
        first = validate_dataframe(
            object(),
            **dataframe_kwargs(FakeDataFrameEngine(PASS_RESULT), InMemoryResultStore()),
        )
        second = validate_dataframe(
            object(),
            **dataframe_kwargs(FakeDataFrameEngine(PASS_RESULT), InMemoryResultStore()),
        )
        self.assertEqual(first.validation_id, second.validation_id)

    def test_table_interface_requires_and_preserves_snapshot_identity(self):
        engine = FakeTableEngine(PASS_RESULT)
        summary = validate_table(
            data_asset_ref="lakehouse.silver.teaching_progress",
            dataset_id="education.teaching_progress",
            data_layer="silver",
            contract_version="1.0.0",
            suite_names=("teaching_progress",),
            suite_versions={"teaching_progress": "1.0.0"},
            run_id="table-run",
            nessie_branch="candidate_table",
            snapshot_id="789",
            nessie_commit_hash="def456",
            validator=engine,
            result_store=InMemoryResultStore(),
        )

        self.assertEqual(summary.execution_engine, ExecutionEngine.SQL)
        self.assertEqual(engine.request.snapshot_id, "789")

    def test_sql_request_without_snapshot_identity_is_rejected(self):
        with self.assertRaisesRegex(ValidationConfigurationError, "snapshot"):
            ValidationRequest(
                data_asset_ref="lakehouse.silver.table",
                dataset_id="dataset",
                data_layer="silver",
                execution_engine=ExecutionEngine.SQL,
                contract_version="1.0.0",
                suites=SUITES,
                run_id="run",
            )

    def test_public_api_rejects_missing_suite_version(self):
        kwargs = dataframe_kwargs(FakeDataFrameEngine(PASS_RESULT), InMemoryResultStore())
        kwargs["suite_versions"] = {}
        with self.assertRaisesRegex(ValidationConfigurationError, "missing_versions"):
            validate_dataframe(object(), **kwargs)


class SQLAdapterTest(unittest.TestCase):
    def test_wraps_unexpected_driver_failure(self):
        adapter = SqlAlchemyTableEngine(
            lambda request: (_ for _ in ()).throw(OSError("connection lost"))
        )
        request = ValidationRequest(
            data_asset_ref="lakehouse.silver.table",
            dataset_id="dataset",
            data_layer="silver",
            execution_engine=ExecutionEngine.SQL,
            contract_version="1.0.0",
            suites=SUITES,
            run_id="run",
            nessie_branch="candidate",
            snapshot_id="1",
            nessie_commit_hash="abc",
        )
        with self.assertRaises(ValidationExecutionError):
            adapter.validate_table(request)


class ResultStoreTest(unittest.TestCase):
    def test_runtime_store_rejects_local_filesystem(self):
        with patch.dict(
            os.environ, {"GX_RESULT_STORE_BACKEND": "filesystem"}, clear=True
        ):
            with self.assertRaisesRegex(ValidationConfigurationError, "filesystem"):
                result_store_from_env()

    def test_s3_store_writes_json_summary(self):
        summary = validate_dataframe(
            object(),
            **dataframe_kwargs(FakeDataFrameEngine(PASS_RESULT), InMemoryResultStore()),
        )
        client = MagicMock()
        boto3_module = MagicMock()
        boto3_module.client.return_value = client
        with patch.dict("sys.modules", {"boto3": boto3_module}):
            uri = S3ResultStore("bucket", "gx/results").save(summary)

        self.assertTrue(uri.startswith("s3://bucket/gx/results/"))
        kwargs = client.put_object.call_args.kwargs
        self.assertEqual(kwargs["ContentType"], "application/json")
        self.assertEqual(json.loads(kwargs["Body"])["snapshot_id"], "456")

    def test_s3_store_fails_closed(self):
        summary = validate_dataframe(
            object(),
            **dataframe_kwargs(FakeDataFrameEngine(PASS_RESULT), InMemoryResultStore()),
        )
        boto3_module = MagicMock()
        boto3_module.client.return_value.put_object.side_effect = OSError("down")
        with patch.dict("sys.modules", {"boto3": boto3_module}):
            with self.assertRaises(ValidationPersistenceError):
                S3ResultStore("bucket").save(summary)


class SuiteRegistryTest(unittest.TestCase):
    def test_registry_enforces_version_engine_and_definition(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            (root / "suite.json").write_text("{}", encoding="utf-8")
            (root / "registry.json").write_text(
                json.dumps(
                    {
                        "suites": [
                            {
                                "name": "teaching_progress",
                                "version": "1.0.0",
                                "supported_engines": ["spark"],
                                "contract_version": "1.0.0",
                                "effective_from": "2026-10-27",
                                "definition": "suite.json",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            registry = SuiteRegistry(root / "registry.json")
            metadata = registry.resolve(SUITES[0], ExecutionEngine.SPARK)

            self.assertEqual(metadata.contract_version, "1.0.0")
            with self.assertRaises(ValidationConfigurationError):
                registry.resolve(SUITES[0], ExecutionEngine.SQL)
            with self.assertRaises(SuiteNotFoundError):
                registry.resolve(SuiteReference("missing", "1.0.0"), ExecutionEngine.SPARK)


if __name__ == "__main__":
    unittest.main()
