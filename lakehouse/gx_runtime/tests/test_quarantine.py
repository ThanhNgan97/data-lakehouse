from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.quarantine import QuarantineManifest, QuarantineStatus
from gx_runtime.api import TableValidationApplication
from gx_runtime.tests.test_snapshot_gate import summary
from gx_runtime.bronze import BronzeValidationResult
try:
    from gx_approval_receipt import sign_approval_receipt
except ModuleNotFoundError:
    from spark.gx_approval_receipt import sign_approval_receipt


class CandidateQuarantineTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(
            os.environ,
            {
                "GX_APPROVAL_SIGNING_KEY_ID": "test-key",
                "GX_APPROVAL_PRIVATE_KEY": "mhhz/j9TRR3mbxkFcd8Alqxr7N4NBZUtj6gXGn7xt+Y=",
                "GX_APPROVAL_PUBLIC_KEYS": '{"test-key":"7VfexdIjq7grSVgW/4x+kP4d1wg+296V+txcBCOhHOA="}',
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_failed_summary_becomes_traceable_quarantine(self):
        manifest = QuarantineManifest.from_summary(
            summary(), "s3://bucket/result.json", reason="DATA_QUALITY_FAILED"
        )
        self.assertEqual(manifest.status, QuarantineStatus.QUARANTINED)
        self.assertEqual(manifest.candidate_hash, "abc123")
        self.assertEqual(manifest.data_asset_ref, "lakehouse.silver.teaching_progress")

    def test_release_requires_revalidation_and_exact_signed_approval(self):
        manifest = QuarantineManifest.from_summary(
            summary(), "s3://bucket/result.json", reason="DATA_QUALITY_FAILED"
        )
        with self.assertRaisesRegex(ValidationConfigurationError, "awaiting"):
            manifest.approve_for_release("operator", {})
        pending = manifest.request_revalidation("operator")
        approval = sign_approval_receipt(
            {
                "validation_id": "fresh-validation",
                "dataset_id": pending.dataset_id,
                "data_layer": pending.data_layer,
                "contract_version": pending.contract_version,
                "run_id": pending.run_id,
                "source_kind": pending.source_kind,
                "data_asset_ref": pending.data_asset_ref,
                "validated_branch": pending.candidate_branch,
                "validated_snapshot_id": pending.candidate_snapshot_id,
                "validated_commit_hash": pending.candidate_hash,
                "candidate_hash": pending.candidate_hash,
                "audience": "silver-publisher",
            }
        )
        released = pending.approve_for_release("operator", approval)
        self.assertEqual(released.status, QuarantineStatus.APPROVED_FOR_RELEASE)
        self.assertEqual(released.validation_id, "fresh-validation")

    def test_revalidate_executes_gx_and_approves_exact_candidate(self):
        manifest = QuarantineManifest.from_summary(
            summary(), "s3://bucket/result.json", reason="DATA_QUALITY_FAILED"
        )
        approval = sign_approval_receipt(
            {
                "validation_id": "fresh-validation",
                "dataset_id": manifest.dataset_id,
                "data_layer": manifest.data_layer,
                "contract_version": manifest.contract_version,
                "run_id": manifest.run_id,
                "source_kind": manifest.source_kind,
                "data_asset_ref": manifest.data_asset_ref,
                "validated_branch": manifest.candidate_branch,
                "validated_snapshot_id": manifest.candidate_snapshot_id,
                "validated_commit_hash": manifest.candidate_hash,
                "candidate_hash": manifest.candidate_hash,
                "audience": "silver-publisher",
            }
        )

        class Store:
            current = manifest
            etag = "etag-1"

            def load(self, _dataset_id, _quarantine_id):
                return self.current, self.etag

            def save(self, value, *, if_match=None):
                self.assert_match(if_match)
                self.current = value
                self.etag = "etag-2" if self.etag == "etag-1" else "etag-3"
                return "s3://bucket/quarantine.json"

            def assert_match(self, supplied):
                if supplied != self.etag:
                    raise AssertionError("stale ETag")

        application = TableValidationApplication.__new__(TableValidationApplication)
        application.http_security = MagicMock()
        application.quarantine = Store()
        application.routes = MagicMock()
        application.routes.resolve.return_value = SimpleNamespace(
            data_asset_ref=manifest.data_asset_ref
        )
        application.validate = MagicMock(
            return_value={
                "status": "PASSED",
                "validation_id": "fresh-validation",
                "approval": approval,
            }
        )
        result = application.transition_quarantine(
            f"/v1/quarantine/{manifest.dataset_id}/{manifest.quarantine_id}/revalidate",
            {},
            caller="gx-admin",
        )
        request = application.validate.call_args.args[0]
        self.assertEqual(request["nessie_branch"], manifest.candidate_branch)
        self.assertEqual(request["snapshot_id"], manifest.candidate_snapshot_id)
        self.assertEqual(request["nessie_commit_hash"], manifest.candidate_hash)
        self.assertFalse(application.validate.call_args.kwargs["persist_quarantine"])
        self.assertEqual(result["status"], QuarantineStatus.APPROVED_FOR_RELEASE.value)

    @patch("boto3.client")
    def test_initial_manifest_write_is_create_only(self, client):
        from gx_runtime.quarantine import S3QuarantineStore

        manifest = QuarantineManifest.from_summary(
            summary(), "s3://bucket/result.json", reason="DATA_QUALITY_FAILED"
        )
        S3QuarantineStore("bucket").save(manifest)
        self.assertEqual(client.return_value.put_object.call_args.kwargs["IfNoneMatch"], "*")

    def test_failed_bronze_result_quarantines_raw_artifact(self):
        result = BronzeValidationResult(
            validation_id="bronze-validation",
            dataset_id="education.teaching_progress",
            contract_version="1.0.0",
            data_asset_ref="lakehouse.bronze.teaching_progress",
            source_kind="file",
            run_id="manual__bronze",
            artifact_ref="s3://bucket/staging/input.csv",
            status="FAILED",
            merge_allowed=False,
            evaluated_expectations=1,
            failures=({"severity": "critical"},),
            metrics={"readable": False},
            validated_at="2026-10-29T00:00:00+00:00",
        )
        manifest = QuarantineManifest.from_bronze_result(
            result, "s3://bucket/results/bronze.json"
        )
        self.assertEqual(manifest.status, QuarantineStatus.QUARANTINED)
        self.assertEqual(manifest.data_layer, "bronze")
        self.assertIsNone(manifest.candidate_branch)


if __name__ == "__main__":
    unittest.main()
