from __future__ import annotations

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from gx_receipt_audit import record_receipt_consumption


class ReceiptConsumptionAuditTest(unittest.TestCase):
    def test_audit_is_create_only_and_contains_no_private_key(self):
        client = MagicMock()
        client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
            "HeadObject",
        )
        approval = {
            "validation_id": "validation-1",
            "dataset_id": "education.teaching_progress",
            "data_layer": "silver",
            "run_id": "manual__1",
            "validated_branch": "candidate-1",
            "validated_snapshot_id": "snapshot-1",
            "validated_commit_hash": "commit-1",
            "signature": "signed-value",
            "key_id": "production-key",
        }
        with patch.dict(
            os.environ,
            {
                "GX_RECEIPT_AUDIT_ENABLED": "true",
                "GX_RESULT_STORE_BUCKET": "quality",
            },
        ):
            uri = record_receipt_consumption(
                approval,
                publication_hash="published-1",
                status="PUBLISHED",
                client=client,
            )
        call = client.put_object.call_args.kwargs
        payload = json.loads(call["Body"])
        self.assertEqual(payload["publication_hash"], "published-1")
        self.assertNotIn("signature", payload)
        self.assertTrue(uri.startswith("s3://quality/gx/receipt-consumption/"))

    def test_existing_audit_is_reused_without_overwrite(self):
        client = MagicMock()
        approval = {
            "validation_id": "validation-1", "dataset_id": "education.teaching_progress",
            "data_layer": "silver", "run_id": "manual__1", "validated_branch": "candidate-1",
            "validated_snapshot_id": "snapshot-1", "validated_commit_hash": "commit-1",
            "signature": "signed-value", "key_id": "production-key",
        }
        with patch.dict(os.environ, {"GX_RECEIPT_AUDIT_ENABLED": "true", "GX_RESULT_STORE_BUCKET": "quality"}):
            record_receipt_consumption(approval, publication_hash="published-1", status="PUBLISHED", client=client)
        client.put_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()
