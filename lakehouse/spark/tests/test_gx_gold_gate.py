import os
import unittest
from unittest.mock import patch

import gx_gold_gate as gate
from api_dataset_registry import LEARNING_OUTCOMES_DATASET
from gx_universal_gate import SnapshotIdentity


class GXGoldGateTest(unittest.TestCase):
    def test_request_targets_gold_with_exact_identity(self):
        payload = gate.build_gold_validation_request(
            dataset_id=LEARNING_OUTCOMES_DATASET,
            run_id="portal_api__learning",
            contract_version="1.0.0",
            source_kind="api",
            identity=SnapshotIdentity("main", "gold-snapshot", "gold-commit"),
        )
        self.assertEqual(payload["data_layer"], "gold")
        self.assertEqual(payload["source_kind"], "api")
        self.assertEqual(payload["snapshot_id"], "gold-snapshot")
        self.assertEqual(payload["nessie_commit_hash"], "gold-commit")

    @patch("gx_gold_gate.execute_quality_gate")
    @patch("gx_gold_gate.resolve_snapshot_identity")
    def test_published_gold_is_validated_and_bounded(self, resolve, execute):
        resolve.return_value = SnapshotIdentity("main", "42", "commit-42")
        execute.return_value = {
            "status": "PASSED",
            "validation_id": "gold-validation",
            "validation_uri": "s3://bucket/gold.json",
            "merge_allowed": True,
            "approval": {},
        }
        with patch.dict(os.environ, {"GX_GOLD_GATE_MODE": "enforce"}):
            result = gate.validate_published_gold(
                dataset_id=LEARNING_OUTCOMES_DATASET,
                run_id="portal_api__learning",
            )
        resolve.assert_called_once_with(
            "lakehouse.gold.learning_outcomes_metrics", "main"
        )
        payload = execute.call_args.args[0]
        self.assertEqual(payload["data_layer"], "gold")
        self.assertEqual(payload["execution_mode"], "enforce")
        self.assertEqual(result["data_asset_ref"], "lakehouse.gold.learning_outcomes_metrics")
        self.assertEqual(result["integration_status"], "PASSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
