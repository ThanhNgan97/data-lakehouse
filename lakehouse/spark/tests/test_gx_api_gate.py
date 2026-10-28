import unittest
from unittest.mock import patch

import gx_api_gate as gate
from api_dataset_registry import TEACHING_PROGRESS_DATASET
from gx_universal_gate import SnapshotIdentity
from gx_candidate_state import CandidateAssetIdentity, CandidateState


class GxApiGateTest(unittest.TestCase):
    @patch("gx_api_gate.execute_quality_gate")
    @patch("gx_api_gate.resolve_snapshot_identity")
    @patch("gx_api_gate.load_candidate_state")
    def test_candidate_gate_consumes_exact_recorded_identity(
        self, load_state, resolve_identity, execute_gate,
    ):
        load_state.return_value = CandidateState(
            dataset_id=TEACHING_PROGRESS_DATASET,
            source_kind="api",
            run_id="manual__candidate",
            candidate_branch="gx_api_silver_0123456789abcdef",
            base_reference="main",
            base_hash="base-1",
            candidate_hash="candidate-1",
            assets=(
                CandidateAssetIdentity(
                    "lakehouse.silver.teaching_progress", "snapshot-1"
                ),
                CandidateAssetIdentity(
                    "lakehouse.silver.teaching_progress_quarantine", "snapshot-q"
                ),
            ),
        )
        resolve_identity.return_value = SnapshotIdentity(
            "gx_api_silver_0123456789abcdef", "snapshot-1", "candidate-1"
        )
        execute_gate.return_value = {
            "status": "PASSED",
            "validation_id": "validation-candidate",
            "validation_uri": "s3://bucket/candidate.json",
            "merge_allowed": True,
            "approval": {},
        }

        result = gate.validate_api_candidate_dataset(
            dataset_id=TEACHING_PROGRESS_DATASET,
            run_id="manual__candidate",
        )

        payload = execute_gate.call_args.args[0]
        self.assertEqual(payload["nessie_branch"], "gx_api_silver_0123456789abcdef")
        self.assertEqual(payload["snapshot_id"], "snapshot-1")
        self.assertEqual(payload["nessie_commit_hash"], "candidate-1")
        self.assertEqual(execute_gate.call_args.kwargs["snapshot_refreshes"], 0)
        self.assertEqual(result["base_hash"], "base-1")

    @patch("gx_api_gate.execute_quality_gate")
    @patch("gx_api_gate.resolve_snapshot_identity")
    @patch("gx_api_gate.load_candidate_state")
    def test_candidate_gate_rejects_state_changed_after_writer(
        self, load_state, resolve_identity, execute_gate,
    ):
        load_state.return_value = CandidateState(
            dataset_id=TEACHING_PROGRESS_DATASET,
            source_kind="api",
            run_id="manual__candidate",
            candidate_branch="gx_api_silver_0123456789abcdef",
            base_reference="main",
            base_hash="base-1",
            candidate_hash="candidate-1",
            assets=(CandidateAssetIdentity(
                "lakehouse.silver.teaching_progress", "snapshot-1"
            ),),
        )
        resolve_identity.return_value = SnapshotIdentity(
            "gx_api_silver_0123456789abcdef", "snapshot-2", "candidate-2"
        )

        with self.assertRaisesRegex(RuntimeError, "stale"):
            gate.validate_api_candidate_dataset(
                dataset_id=TEACHING_PROGRESS_DATASET,
                run_id="manual__candidate",
            )
        execute_gate.assert_not_called()

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
        self.assertEqual(request["data_asset_ref"], "lakehouse.silver.teaching_progress")
        self.assertEqual(request["snapshot_id"], "snapshot-2")
        self.assertEqual(result["status"], "PASSED")
        self.assertEqual(result["integration_status"], "PASSED")
        self.assertTrue(result["merge_allowed"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
