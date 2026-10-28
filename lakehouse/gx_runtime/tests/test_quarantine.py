from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.quarantine import QuarantineManifest, QuarantineStatus
from gx_runtime.tests.test_snapshot_gate import summary
try:
    from gx_approval_receipt import sign_approval_receipt
except ModuleNotFoundError:
    from spark.gx_approval_receipt import sign_approval_receipt


class CandidateQuarantineTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(
            os.environ,
            {"GX_APPROVAL_SECRET": "unit-test-secret-that-is-at-least-32-chars"},
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


if __name__ == "__main__":
    unittest.main()
