import unittest
from unittest.mock import patch

import gx_api_gate as gate
from api_dataset_registry import TEACHING_PROGRESS_DATASET
from gx_universal_gate import SnapshotIdentity


class GxApiGateTest(unittest.TestCase):
    def test_request_carries_api_provenance_and_exact_identity(self):
        identity = SnapshotIdentity("main", "snapshot-1", "commit-1")

        payload = gate.build_api_validation_request(
            dataset_id=TEACHING_PROGRESS_DATASET,
            run_id="manual__api-test",
            contract_version="1.0.0",
            identity=identity,
        )

        self.assertEqual(payload["source_kind"], "api")
        self.assertEqual(payload["data_layer"], "silver")
        self.assertEqual(payload["snapshot_id"], "snapshot-1")
        self.assertEqual(payload["nessie_commit_hash"], "commit-1")
        self.assertEqual(payload["execution_mode"], "enforce")

    @patch("gx_api_gate.execute_quality_gate")
    @patch("gx_api_gate.resolve_snapshot_identity")
    def test_registered_dataset_is_validated_before_gold(
        self, resolve_identity, execute_gate,
    ):
        resolve_identity.return_value = SnapshotIdentity(
            "main", "snapshot-2", "commit-2"
        )
        execute_gate.return_value = {
            "status": "PASSED",
            "validation_id": "validation-2",
            "validation_uri": "s3://bucket/result.json",
            "merge_allowed": True,
            "approval": {},
        }

        result = gate.validate_api_dataset(
            dataset_id=TEACHING_PROGRESS_DATASET,
            run_id="manual__api-test",
        )

        resolve_identity.assert_called_once_with(
            "lakehouse.silver.teaching_progress", "main"
        )
        request = execute_gate.call_args.args[0]
        self.assertEqual(request["source_kind"], "api")
        self.assertEqual(request["snapshot_id"], "snapshot-2")
        self.assertEqual(result["status"], "PASSED")
        self.assertEqual(result["integration_status"], "PASSED")
        self.assertTrue(result["merge_allowed"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
