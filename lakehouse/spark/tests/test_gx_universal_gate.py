from __future__ import annotations

import io
import json
import os
import unittest
from unittest.mock import MagicMock, patch

import gx_universal_gate as gate


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class GXUniversalGateTest(unittest.TestCase):
    def test_resolves_exact_branch_snapshot_and_commit(self):
        opener = MagicMock(
            side_effect=[
                Response(json.dumps({"type": "BRANCH", "hash": "commit-1"}).encode()),
                Response(json.dumps({"type": "ICEBERG_TABLE", "snapshotId": 42}).encode()),
            ]
        )
        identity = gate.resolve_snapshot_identity(
            "lakehouse.silver.teaching_progress",
            "candidate-1",
            nessie_url="http://nessie/api/v1",
            opener=opener,
        )
        self.assertEqual(identity.snapshot_id, "42")
        self.assertEqual(identity.commit_hash, "commit-1")
        self.assertIn("silver.teaching_progress", opener.call_args_list[1].args[0])

    @patch("gx_universal_gate.execute_quality_gate")
    @patch("gx_universal_gate.resolve_snapshot_identity")
    def test_unregistered_dynamic_upload_runs_baseline_gx(self, resolve, execute):
        resolve.return_value = gate.SnapshotIdentity("main", "7", "commit-7")
        execute.return_value = {
            "status": "PASSED", "validation_id": "baseline-7",
            "validation_uri": "s3://bucket/baseline.json", "merge_allowed": True,
            "approval": {}, "coverage": "BASELINE", "contract_status": "MISSING",
        }
        ti = MagicMock()
        ti.xcom_pull.side_effect = [None, "lakehouse.silver.dynamic_table"]

        result = gate.validate_file_upload(ti=ti, run_id="run-1")

        payload = execute.call_args.args[0]
        self.assertEqual(payload["coverage"], "BASELINE")
        self.assertEqual(payload["contract_version"], "baseline-1.0.0")
        self.assertEqual(payload["data_asset_ref"], "lakehouse.silver.dynamic_table")
        self.assertEqual(result["contract_status"], "MISSING")

    @patch("gx_universal_gate.execute_quality_gate")
    @patch("gx_universal_gate.resolve_snapshot_identity")
    def test_registered_file_upload_currently_resolves_main_by_default(
        self, resolve, execute
    ):
        """Characterize the phase-A gap: upload GX is post-write on main."""
        resolve.return_value = gate.SnapshotIdentity("main", "42", "commit-1")
        execute.return_value = {
            "status": "PASSED",
            "validation_id": "validation-1",
            "validation_uri": "s3://bucket/result.json",
            "merge_allowed": True,
            "approval": {"approved": True},
        }
        ti = MagicMock()
        ti.xcom_pull.side_effect = [
            "education.teaching_progress",
            "lakehouse.silver.teaching_progress",
        ]

        with patch.dict(os.environ, {}, clear=True):
            gate.validate_file_upload(ti=ti, run_id="run-1")

        resolve.assert_called_once_with(
            "lakehouse.silver.teaching_progress", "main"
        )

    @patch("gx_universal_gate.execute_quality_gate")
    @patch("gx_universal_gate.resolve_snapshot_identity")
    def test_registered_file_upload_calls_gx_with_snapshot_identity(
        self, resolve, execute
    ):
        resolve.return_value = gate.SnapshotIdentity("main", "42", "commit-1")
        execute.return_value = {
            "status": "PASSED",
            "validation_id": "validation-1",
            "validation_uri": "s3://bucket/result.json",
            "merge_allowed": True,
            "approval": {"approved": True},
        }
        ti = MagicMock()
        ti.xcom_pull.side_effect = [
            "education.teaching_progress",
            "lakehouse.silver.teaching_progress",
        ]
        with patch.dict(os.environ, {"GX_FILE_GATE_MODE": "enforce"}, clear=False):
            result = gate.validate_file_upload(ti=ti, run_id="run-1")
        payload = execute.call_args.args[0]
        self.assertEqual(payload["source_kind"], "file")
        self.assertEqual(payload["snapshot_id"], "42")
        self.assertEqual(payload["execution_mode"], "enforce")
        self.assertEqual(payload["coverage"], "CONTRACT")
        self.assertEqual(result["integration_status"], "PASSED")


if __name__ == "__main__":
    unittest.main()
