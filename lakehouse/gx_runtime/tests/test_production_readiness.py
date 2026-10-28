from __future__ import annotations

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.models import ExecutionMode, ExpectationFailure
from gx_runtime.production import ProductionSettings
from gx_runtime.revocation import S3SnapshotRevocationStore, SnapshotRevocation
from gx_runtime.storage import S3ResultStore

from gx_runtime.tests.test_snapshot_gate import summary
from gx_runtime.tests.test_framework import FakeDataFrameEngine, PASS_RESULT, dataframe_kwargs
from gx_runtime.service import validate_dataframe
from gx_runtime.storage import InMemoryResultStore


class ProductionReadinessTest(unittest.TestCase):
    def test_production_requires_enforce_revision_and_redaction(self):
        settings = ProductionSettings("production", "abc123", 90, True)
        settings.validate_mode(ExecutionMode.ENFORCE)
        with self.assertRaisesRegex(ValidationConfigurationError, "enforce"):
            settings.validate_mode(ExecutionMode.REPORT)

    @patch.dict(os.environ, {"GX_ENVIRONMENT": "production"}, clear=True)
    def test_production_requires_auditable_suite_revision(self):
        with self.assertRaisesRegex(ValidationConfigurationError, "GX_SUITE_REVISION"):
            ProductionSettings.from_env()

    @patch.dict(
        os.environ,
        {
            "GX_ENVIRONMENT": "production",
            "GX_SUITE_REVISION": "git-sha",
            "GX_REDACT_OBSERVED_VALUES": "false",
        },
        clear=True,
    )
    def test_production_cannot_disable_redaction(self):
        with self.assertRaisesRegex(ValidationConfigurationError, "must be true"):
            ProductionSettings.from_env()

    @patch.dict(
        os.environ,
        {
            "GX_ENVIRONMENT": "production",
            "GX_SUITE_REVISION": "git-sha",
            "GX_REDACT_OBSERVED_VALUES": "true",
            "GX_APPROVAL_SECRET": "development-only-gx-approval-secret-change-me",
        },
        clear=True,
    )
    def test_production_refuses_default_approval_secret(self):
        with self.assertRaisesRegex(ValidationConfigurationError, "non-default"):
            ProductionSettings.from_env()

    def test_summary_redacts_observed_values(self):
        item = summary()
        object.__setattr__(
            item,
            "critical_failures",
            (ExpectationFailure("unexpected", "critical", "email", "secret@example"),),
        )
        payload = item.as_dict(redact_observed_values=True)
        self.assertEqual(
            payload["critical_failures"][0]["observed_value"], "[REDACTED]"
        )

    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {"GX_REDACT_OBSERVED_VALUES": "true", "GX_RESULT_RETENTION_DAYS": "30"},
        clear=True,
    )
    def test_s3_result_is_redacted_tagged_and_revision_audited(self, client):
        item = summary()
        object.__setattr__(item, "suite_revision", "commit-123")
        object.__setattr__(
            item,
            "critical_failures",
            (ExpectationFailure("unexpected", observed_value="private"),),
        )
        S3ResultStore("quality-bucket").save(item)

        call = client.return_value.put_object.call_args.kwargs
        self.assertEqual(call["Metadata"]["gx-suite-revision"], "commit-123")
        self.assertIn("gx-retention-days=30", call["Tagging"])
        self.assertNotIn("private", call["Body"].decode("utf-8"))
        json.loads(call["Body"])

    @patch("boto3.client")
    def test_snapshot_revocation_is_durable_and_contains_no_row_data(self, client):
        revocation = SnapshotRevocation(
            dataset_id="education.teaching_progress",
            data_layer="silver",
            nessie_branch="candidate-1",
            revoked_snapshot_id="old-snapshot",
            revoked_commit_hash="old-commit",
            current_snapshot_id="new-snapshot",
            current_commit_hash="new-commit",
        )
        uri = S3SnapshotRevocationStore("quality-bucket").save(revocation)

        call = client.return_value.put_object.call_args.kwargs
        payload = json.loads(call["Body"])
        self.assertTrue(uri.startswith("s3://quality-bucket/gx/revocations/"))
        self.assertEqual(payload["reason"], "SNAPSHOT_MISMATCH")
        self.assertNotIn("rows", payload)

    def test_suite_revision_changes_validation_identity_for_safe_rollback(self):
        kwargs = dataframe_kwargs(FakeDataFrameEngine(PASS_RESULT), InMemoryResultStore())
        first = validate_dataframe(object(), **kwargs, suite_revision="revision-a")
        kwargs = dataframe_kwargs(FakeDataFrameEngine(PASS_RESULT), InMemoryResultStore())
        second = validate_dataframe(object(), **kwargs, suite_revision="revision-b")

        self.assertNotEqual(first.validation_id, second.validation_id)


if __name__ == "__main__":
    unittest.main()
