from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import gx_bronze_gate as gate


class BronzeEvidenceCollectorTest(unittest.TestCase):
    def temporary_directory(self):
        return tempfile.TemporaryDirectory(dir=Path(__file__).parent)

    def test_valid_csv_produces_complete_file_evidence(self):
        with self.temporary_directory() as directory:
            source = Path(directory) / "input.csv"
            source.write_text("id,value\n1,ok\n", encoding="utf-8")
            metrics = gate.inspect_file(str(source))
        self.assertTrue(metrics["readable"])
        self.assertTrue(metrics["header_valid"])
        self.assertTrue(metrics["checksum_present"])
        self.assertEqual(metrics["row_count"], 1)

    def test_invalid_json_is_reported_as_corrupt_not_passed(self):
        with self.temporary_directory() as directory:
            source = Path(directory) / "input.json"
            source.write_text("{broken", encoding="utf-8")
            metrics = gate.inspect_file(str(source))
        self.assertFalse(metrics["readable"])
        self.assertEqual(metrics["corrupt_record_ratio"], 1.0)

    @patch("gx_bronze_gate.execute_bronze_quality_gate")
    @patch("gx_bronze_gate.resolve_route")
    def test_file_gate_resolves_contract_before_calling_service(self, resolve, execute):
        resolve.return_value.contract_version = "1.0.0"
        execute.return_value = {"status": "PASSED", "merge_allowed": True}
        with self.temporary_directory() as directory:
            source = Path(directory) / "input.csv"
            source.write_text("id\n1\n", encoding="utf-8")
            result = gate.validate_file_bronze(
                dataset_id="education.teaching_progress",
                run_id="manual__1",
                path=str(source),
            )
        self.assertTrue(result["merge_allowed"])
        resolve.assert_called_once_with(
            dataset_id="education.teaching_progress",
            data_layer="bronze",
            source_kind="file",
            execution_engine="sql",
        )


if __name__ == "__main__":
    unittest.main()
