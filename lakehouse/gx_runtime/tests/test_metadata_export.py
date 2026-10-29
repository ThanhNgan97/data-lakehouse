from __future__ import annotations

import io
import json
import os
import unittest
from unittest.mock import patch

from gx_runtime.errors import MetadataExportError, ValidationConfigurationError
from gx_runtime.metadata_export import (
    OpenMetadataTestCaseAdapter,
    events_from_summary,
    export_validation_metadata,
    openmetadata_exporter_from_env,
)
from gx_runtime.models import (
    EngineValidationResult,
    ExecutionEngine,
    SourceKind,
    SuiteReference,
    ValidationRequest,
)
from gx_runtime.policy import ValidationPolicy


def validation_summary():
    request = ValidationRequest(
        data_asset_ref="lakehouse.gold.teaching_progress_metrics",
        dataset_id="education.teaching_progress",
        data_layer="gold",
        execution_engine=ExecutionEngine.SQL,
        contract_version="1.0.0",
        suites=(
            SuiteReference("teaching_progress_gold_core", "1.0.0"),
            SuiteReference("teaching_progress_reconciliation", "1.1.0"),
        ),
        run_id="phase9-test",
        nessie_branch="candidate",
        snapshot_id="123",
        nessie_commit_hash="abc",
        source_kind=SourceKind.API,
    )
    return ValidationPolicy().apply(request, EngineValidationResult(True, 2, 2))


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
        return False


class ValidationMetadataContractTest(unittest.TestCase):
    def test_one_event_per_suite_contains_required_traceability_only(self):
        events = events_from_summary(
            validation_summary(), "s3://bucket/validation.json"
        )
        self.assertEqual(len(events), 2)
        first = events[0].as_dict()
        self.assertEqual(
            set(first),
            {
                "dataset", "layer", "run_id", "suite_name", "suite_version",
                "status", "branch", "snapshot_id", "commit_hash",
                "data_docs_url", "validation_timestamp", "validation_id",
                "validation_uri",
            },
        )
        self.assertEqual(first["dataset"], "education.teaching_progress")
        self.assertEqual(first["suite_version"], "1.0.0")
        self.assertNotIn("critical_failures", first)
        self.assertNotIn("observed_value", first)

    def test_missing_snapshot_identity_is_rejected(self):
        summary = validation_summary()
        object.__setattr__(summary, "snapshot_id", None)
        with self.assertRaisesRegex(ValidationConfigurationError, "snapshot_id"):
            events_from_summary(summary, "s3://bucket/result.json")


class OpenMetadataAdapterTest(unittest.TestCase):
    def test_adapter_emits_test_case_result_with_auth_and_gx_fields(self):
        calls = []

        def opener(call, **kwargs):
            calls.append((call, kwargs))
            return Response(b"{}")

        event = events_from_summary(
            validation_summary(), "s3://bucket/result.json"
        )[0]
        reference = OpenMetadataTestCaseAdapter(
            "http://openmetadata:8585", "secret", opener=opener
        ).publish(event)

        call, kwargs = calls[0]
        payload = json.loads(call.data)
        values = {item["name"]: item["value"] for item in payload["testResultValue"]}
        self.assertEqual(call.method, "POST")
        self.assertEqual(call.headers["Authorization"], "Bearer secret")
        self.assertIn("/api/v1/dataQuality/testCases/testCaseResults/", call.full_url)
        self.assertEqual(payload["testCaseStatus"], "Success")
        self.assertEqual(values["snapshot_id"], "123")
        self.assertEqual(values["commit_hash"], "abc")
        self.assertEqual(kwargs["timeout"], 10)
        self.assertIn("gx_teaching_progress_gold_core_1_0_0", reference)

    def test_best_effort_records_failure_but_required_mode_raises(self):
        class BrokenExporter:
            def publish(self, _event):
                raise OSError("offline")

        best_effort = export_validation_metadata(
            validation_summary(),
            "s3://bucket/result.json",
            BrokenExporter(),
        )
        self.assertEqual(best_effort.published, 0)
        self.assertEqual(best_effort.failed, 2)
        with self.assertRaises(MetadataExportError):
            export_validation_metadata(
                validation_summary(),
                "s3://bucket/result.json",
                BrokenExporter(),
                required=True,
            )

    def test_warning_maps_to_neutral_aborted_status(self):
        event = events_from_summary(
            validation_summary(), "s3://bucket/result.json"
        )[0]
        object.__setattr__(event, "status", "WARNING")
        payload = OpenMetadataTestCaseAdapter._payload(event)
        self.assertEqual(payload["testCaseStatus"], "Aborted")

    def test_legacy_api_style_remains_explicitly_configurable(self):
        calls = []
        event = events_from_summary(
            validation_summary(), "s3://bucket/result.json"
        )[0]
        OpenMetadataTestCaseAdapter(
            "http://openmetadata:8585",
            "secret",
            api_style="legacy",
            opener=lambda call, **_kwargs: (
                calls.append(call) or Response(b"{}")
            ),
        ).publish(event)
        self.assertEqual(calls[0].method, "PUT")
        self.assertIn("/testCaseResult", calls[0].full_url)

    def test_environment_is_disabled_by_default_and_requires_credentials(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(openmetadata_exporter_from_env())
        with patch.dict(
            os.environ,
            {"GX_OPENMETADATA_ENABLED": "true"},
            clear=True,
        ):
            with self.assertRaises(ValidationConfigurationError):
                openmetadata_exporter_from_env()


if __name__ == "__main__":
    unittest.main()
