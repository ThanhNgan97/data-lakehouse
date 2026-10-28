import os
import unittest
from unittest.mock import patch

import gx_reconciliation_gate as gate
from api_dataset_registry import TEACHING_PROGRESS_DATASET
from gx_universal_gate import SnapshotIdentity


class GXReconciliationGateTest(unittest.TestCase):
    @patch("gx_reconciliation_gate.execute_quality_gate")
    @patch("gx_reconciliation_gate.resolve_snapshot_identity")
    def test_reconciliation_anchors_exact_gold_identity(self, resolve, execute):
        resolve.return_value = SnapshotIdentity("main", "43", "commit-43")
        execute.return_value = {
            "status": "PASSED",
            "validation_id": "reconciliation-43",
            "validation_uri": "s3://bucket/reconciliation.json",
            "merge_allowed": True,
            "approval": {},
        }
        with patch.dict(
            os.environ, {"GX_RECONCILIATION_GATE_MODE": "enforce"}, clear=False
        ):
            result = gate.validate_silver_gold_reconciliation(
                dataset_id=TEACHING_PROGRESS_DATASET,
                run_id="portal_api__teaching",
            )
        resolve.assert_called_once_with(
            "lakehouse.gold.teaching_progress_metrics", "main"
        )
        payload = execute.call_args.args[0]
        self.assertEqual(payload["data_layer"], "reconciliation")
        self.assertEqual(payload["data_asset_ref"], "lakehouse.gold.teaching_progress_metrics")
        self.assertEqual(payload["snapshot_id"], "43")
        self.assertEqual(result["silver_asset_ref"], "lakehouse.silver.teaching_progress")
        self.assertEqual(result["gold_asset_ref"], "lakehouse.gold.teaching_progress_metrics")
        self.assertEqual(result["integration_status"], "PASSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
