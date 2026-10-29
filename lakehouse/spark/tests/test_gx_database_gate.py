from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

import gx_database_gate as gate
from gx_candidate_state import CandidateAssetIdentity, CandidateState
from gx_universal_gate import SnapshotIdentity


ASSET = "lakehouse.silver.teaching_progress"


def state(**overrides):
    values = {
        "dataset_id": "education.teaching_progress",
        "source_kind": "database",
        "run_id": "airbyte__42",
        "candidate_branch": "gx_database_silver_0123456789abcdef",
        "base_reference": "main",
        "base_hash": "base-1",
        "candidate_hash": "candidate-1",
        "assets": (CandidateAssetIdentity(ASSET, "snapshot-1"),),
    }
    values.update(overrides)
    return CandidateState(**values)


class GXDatabaseGateTest(unittest.TestCase):
    @patch("gx_database_gate.execute_quality_gate")
    @patch("gx_database_gate.resolve_snapshot_identity")
    @patch("gx_database_gate.load_candidate_state")
    @patch("gx_database_gate.resolve_route")
    def test_registered_candidate_calls_gx_on_sealed_branch(
        self, route, load, resolve, execute
    ):
        route.return_value.data_asset_ref = ASSET
        route.return_value.contract_version = "1.0.0"
        load.return_value = state()
        resolve.return_value = SnapshotIdentity(
            state().candidate_branch, "snapshot-1", "candidate-1"
        )
        execute.return_value = {
            "status": "PASSED", "validation_id": "validation-1",
            "validation_uri": "s3://bucket/result.json", "merge_allowed": True,
            "approval": {},
        }
        with patch.dict(os.environ, {"GX_DATABASE_GATE_MODE": "enforce"}):
            result = gate.validate_database_candidate(
                dataset_id=state().dataset_id, run_id=state().run_id
            )
        payload = execute.call_args.args[0]
        self.assertEqual(payload["source_kind"], "database")
        self.assertEqual(payload["nessie_branch"], state().candidate_branch)
        self.assertEqual(payload["snapshot_id"], "snapshot-1")
        self.assertEqual(execute.call_args.kwargs["snapshot_refreshes"], 0)
        self.assertEqual(result["candidate_hash"], "candidate-1")

    @patch("gx_database_gate.resolve_snapshot_identity")
    @patch("gx_database_gate.load_candidate_state", return_value=state())
    @patch("gx_database_gate.resolve_route")
    def test_changed_candidate_is_rejected_before_gx(self, route, _load, resolve):
        route.return_value.data_asset_ref = ASSET
        route.return_value.contract_version = "1.0.0"
        resolve.return_value = SnapshotIdentity(
            state().candidate_branch, "snapshot-other", "candidate-other"
        )
        with self.assertRaisesRegex(RuntimeError, "changed"):
            gate.validate_database_candidate(
                dataset_id=state().dataset_id, run_id=state().run_id
            )

    def test_unregistered_context_is_not_baseline_passed(self):
        ti = MagicMock()
        ti.xcom_pull.return_value = None
        with self.assertRaisesRegex(RuntimeError, "NOT_CONFIGURED"):
            gate.validate_database_context(ti=ti, run_id="airbyte__42")


if __name__ == "__main__":
    unittest.main()
