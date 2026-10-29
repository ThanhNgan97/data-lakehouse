from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import gx_file_gate as gate
from gx_candidate_state import CandidateAssetIdentity, CandidateState
from gx_universal_gate import SnapshotIdentity


class GXFileGateTest(unittest.TestCase):
    @patch("gx_file_gate.execute_quality_gate")
    @patch("gx_file_gate.resolve_snapshot_identity")
    @patch("gx_file_gate.load_candidate_state")
    @patch("gx_file_gate.resolve_route")
    def test_registered_file_uses_sealed_candidate(
        self, resolve_route, load_state, resolve_identity, execute
    ):
        asset = "lakehouse.silver.teaching_progress"
        resolve_route.return_value = SimpleNamespace(
            data_asset_ref=asset, contract_version="1.0.0"
        )
        load_state.return_value = CandidateState(
            dataset_id="education.teaching_progress",
            source_kind="file",
            run_id="file__1",
            candidate_branch="gx_file_1",
            base_reference="main",
            base_hash="base",
            candidate_hash="sealed",
            assets=(CandidateAssetIdentity(asset, "42"),),
        )
        resolve_identity.return_value = SnapshotIdentity("gx_file_1", "42", "sealed")
        execute.return_value = {"status": "PASSED", "merge_allowed": True}

        result = gate.validate_file_candidate(
            dataset_id="education.teaching_progress", run_id="file__1"
        )

        payload = execute.call_args.args[0]
        self.assertEqual(payload["source_kind"], "file")
        self.assertEqual(payload["nessie_branch"], "gx_file_1")
        self.assertEqual(payload["snapshot_id"], "42")
        self.assertEqual(result["candidate_hash"], "sealed")

    @patch("gx_file_gate.execute_quality_gate")
    @patch("gx_file_gate.resolve_snapshot_identity")
    @patch("gx_file_gate.load_candidate_state")
    @patch("gx_file_gate.resolve_route")
    def test_changed_candidate_is_rejected_before_http(
        self, resolve_route, load_state, resolve_identity, execute
    ):
        asset = "lakehouse.silver.teaching_progress"
        resolve_route.return_value = SimpleNamespace(
            data_asset_ref=asset, contract_version="1.0.0"
        )
        load_state.return_value = CandidateState(
            dataset_id="education.teaching_progress",
            source_kind="file",
            run_id="file__1",
            candidate_branch="gx_file_1",
            base_reference="main",
            base_hash="base",
            candidate_hash="sealed",
            assets=(CandidateAssetIdentity(asset, "42"),),
        )
        resolve_identity.return_value = SnapshotIdentity("gx_file_1", "43", "changed")
        with self.assertRaisesRegex(RuntimeError, "changed"):
            gate.validate_file_candidate(
                dataset_id="education.teaching_progress", run_id="file__1"
            )
        execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
