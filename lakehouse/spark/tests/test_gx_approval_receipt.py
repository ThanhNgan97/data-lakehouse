from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from gx_approval_receipt import (
    GXApprovalReceiptError,
    sign_approval_receipt,
    verify_approval_receipt,
)


class GXApprovalReceiptTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(
            "os.environ",
            {"GX_APPROVAL_SECRET": "unit-test-secret-that-is-at-least-32-chars"},
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.now = datetime(2026, 10, 28, tzinfo=timezone.utc)
        self.claims = {
            "validation_id": "validation-1",
            "dataset_id": "education.teaching_progress",
            "data_layer": "silver",
            "contract_version": "1.0.0",
            "run_id": "run-1",
            "source_kind": "file",
            "data_asset_ref": "lakehouse.silver.teaching_progress",
            "validated_branch": "candidate-1",
            "validated_snapshot_id": "snapshot-1",
            "validated_commit_hash": "commit-1",
            "candidate_hash": "commit-1",
            "audience": "silver-publisher",
        }

    def test_signed_receipt_verifies_exact_claims(self):
        receipt = sign_approval_receipt(self.claims, now=self.now, ttl_seconds=60)
        verified = verify_approval_receipt(
            receipt, expected=self.claims, now=self.now + timedelta(seconds=30)
        )
        self.assertEqual(verified["receipt_version"], "gx-approval/v1")

    def test_tampering_and_expiration_fail_closed(self):
        receipt = sign_approval_receipt(self.claims, now=self.now, ttl_seconds=60)
        tampered = dict(receipt, candidate_hash="other")
        with self.assertRaisesRegex(GXApprovalReceiptError, "signature"):
            verify_approval_receipt(tampered, expected=self.claims, now=self.now)
        with self.assertRaisesRegex(GXApprovalReceiptError, "expired"):
            verify_approval_receipt(
                receipt, expected=self.claims, now=self.now + timedelta(seconds=60)
            )


if __name__ == "__main__":
    unittest.main()
