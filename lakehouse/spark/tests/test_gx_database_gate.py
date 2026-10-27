import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import gx_database_gate as gate
from gx_universal_gate import SnapshotIdentity


class GXDatabaseGateTest(unittest.TestCase):
    def _context(self, dataset_id, table):
        temporary = tempfile.TemporaryDirectory()
        path = Path(temporary.name) / "decision.json"
        path.write_text(json.dumps({"target_silver_table": table}), encoding="utf-8")
        ti = MagicMock()
        ti.xcom_pull.side_effect = [dataset_id, str(path)]
        return temporary, {"ti": ti, "run_id": "airbyte__42"}

    def test_dynamic_context_is_explicitly_not_configured(self):
        temporary, context = self._context(None, "lakehouse.silver.tenant__students")
        with temporary:
            result = gate.validate_database_context(**context)
        self.assertEqual(result["status"], "NOT_CONFIGURED")
        self.assertIsNone(result["merge_allowed"])
        self.assertEqual(result["source_kind"], "database")

    def test_registered_dataset_must_match_canonical_asset(self):
        temporary, context = self._context(
            "education.teaching_progress", "lakehouse.silver.tenant__progress"
        )
        with temporary:
            result = gate.validate_database_context(**context)
        self.assertEqual(result["status"], "NOT_CONFIGURED")
        self.assertIn("does not match", result["reason"])

    @patch("gx_database_gate.execute_quality_gate")
    @patch("gx_database_gate.resolve_snapshot_identity")
    def test_registered_canonical_database_asset_calls_gx(self, resolve, execute):
        resolve.return_value = SnapshotIdentity("main", "42", "commit-42")
        execute.return_value = {
            "status": "PASSED", "validation_id": "validation-42",
            "validation_uri": "s3://bucket/result.json", "merge_allowed": True,
            "approval": {},
        }
        temporary, context = self._context(
            "education.teaching_progress", "lakehouse.silver.teaching_progress"
        )
        with temporary, patch.dict(os.environ, {"GX_DATABASE_GATE_MODE": "enforce"}):
            result = gate.validate_database_context(**context)
        payload = execute.call_args.args[0]
        self.assertEqual(payload["source_kind"], "database")
        self.assertEqual(payload["snapshot_id"], "42")
        self.assertEqual(result["integration_status"], "PASSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
