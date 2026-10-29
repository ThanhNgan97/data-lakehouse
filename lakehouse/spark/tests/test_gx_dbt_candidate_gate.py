from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import gx_dbt_candidate_gate as gate
from gx_candidate_binding import dbt_gold_candidate_binding
from gx_universal_gate import SnapshotIdentity


DATASET = "education.teaching_progress"
RUN_ID = "manual__42"
CANONICAL = "lakehouse.gold.teaching_progress_metrics"


def route(layer: str):
    return SimpleNamespace(
        contract_version="1.0.0",
        data_asset_ref=CANONICAL,
        data_layer=layer,
    )


class GXDbtCandidateGateTest(unittest.TestCase):
    def state(self):
        binding = dbt_gold_candidate_binding(DATASET, RUN_ID, CANONICAL)
        return {
            "phase": "validated",
            "ref": binding.candidate_branch,
            "validated_hash": "commit-42",
            "outputs": {
                "teaching_progress_metrics": {
                    "alias": binding.physical_asset_ref.rsplit(".", 1)[-1]
                }
            },
            "candidate_contents": {
                "teaching_progress_metrics": {"snapshotId": 42}
            },
        }

    @patch("gx_dbt_candidate_gate.execute_quality_gate")
    @patch("gx_dbt_candidate_gate.resolve_snapshot_identity")
    @patch("gx_dbt_candidate_gate.resolve_route")
    @patch("gx_dbt_candidate_gate.load_gold_state")
    @patch("gx_dbt_candidate_gate.get_dataset_config")
    def test_gold_gate_binds_canonical_route_to_sealed_physical_candidate(
        self, get_config, load_state, resolve_route, resolve_identity, execute
    ):
        binding = dbt_gold_candidate_binding(DATASET, RUN_ID, CANONICAL)
        get_config.return_value = object()
        load_state.return_value = self.state()
        resolve_route.return_value = route("gold")
        resolve_identity.return_value = SnapshotIdentity(
            binding.candidate_branch, "42", "commit-42"
        )
        execute.return_value = {"status": "PASSED", "merge_allowed": True}

        result = gate.validate_dbt_gold_candidate(dataset_id=DATASET, run_id=RUN_ID)

        payload = execute.call_args.args[0]
        self.assertEqual(payload["data_asset_ref"], CANONICAL)
        self.assertEqual(payload["physical_data_asset_ref"], binding.physical_asset_ref)
        self.assertEqual(payload["nessie_branch"], binding.candidate_branch)
        self.assertEqual(payload["nessie_commit_hash"], "commit-42")
        self.assertEqual(result["physical_data_asset_ref"], binding.physical_asset_ref)

    @patch("gx_dbt_candidate_gate.execute_quality_gate")
    @patch("gx_dbt_candidate_gate.resolve_snapshot_identity")
    @patch("gx_dbt_candidate_gate.resolve_route")
    @patch("gx_dbt_candidate_gate.load_gold_state")
    @patch("gx_dbt_candidate_gate.get_dataset_config")
    def test_stale_candidate_is_rejected_before_gx_http(
        self, get_config, load_state, resolve_route, resolve_identity, execute
    ):
        binding = dbt_gold_candidate_binding(DATASET, RUN_ID, CANONICAL)
        get_config.return_value = object()
        load_state.return_value = self.state()
        resolve_route.return_value = route("gold")
        resolve_identity.return_value = SnapshotIdentity(
            binding.candidate_branch, "43", "commit-43"
        )

        with self.assertRaisesRegex(RuntimeError, "changed after sealed validation"):
            gate.validate_dbt_gold_candidate(dataset_id=DATASET, run_id=RUN_ID)
        execute.assert_not_called()

    @patch("gx_dbt_candidate_gate._validate_candidate")
    def test_reconciliation_wrapper_selects_reconciliation_route(self, validate):
        validate.return_value = {"status": "PASSED"}
        gate.reconcile_dbt_gold_candidate(dataset_id=DATASET, run_id=RUN_ID)
        validate.assert_called_once_with(
            dataset_id=DATASET,
            run_id=RUN_ID,
            data_layer="reconciliation",
            source_kind="api",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
