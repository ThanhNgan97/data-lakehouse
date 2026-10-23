"""Supported tabular uploads must be routed using their data columns."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


SPARK_DIR = Path(__file__).resolve().parents[1]
if str(SPARK_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_DIR))

from ai_dataset_router import profile_with_rule_fallback, route_from_file_path


class TabularUploadRoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.frame = pd.DataFrame(
            [{"customer_id": "C-1", "region": "North", "amount": 12.5}]
        )

    def _assert_real_columns(self, path: Path):
        with patch(
            "ai_dataset_router.profile_with_gemini",
            side_effect=lambda columns, rows, name: profile_with_rule_fallback(
                columns, rows, suggested_name=name
            ),
        ):
            decision = route_from_file_path(path)
        self.assertEqual(decision.route_target, "generic_dynamic")
        self.assertIn("customer_id", decision.business_keys)
        self.assertIn("amount", decision.metric_columns)
        self.assertNotIn("file_content", decision.business_keys)

    def test_excel_uses_sheet_columns(self):
        path = Path(self.temp_dir.name) / "customers.xlsx"
        self.frame.to_excel(path, index=False)
        self._assert_real_columns(path)

    def test_parquet_uses_schema_columns(self):
        path = Path(self.temp_dir.name) / "customers.parquet"
        self.frame.to_parquet(path, index=False)
        self._assert_real_columns(path)


if __name__ == "__main__":
    unittest.main()
