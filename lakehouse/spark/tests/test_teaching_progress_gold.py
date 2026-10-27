"""Teaching Gold receipt contract; business row checks live in dbt SQL tests."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, os.environ.get("DBT_SERVICE_DIR", str(Path(__file__).resolve().parents[2] / "dbt/service")))
from result_gate import TEACHING_TESTS, verify_results
from dbt_gold_contract import identity, outputs


class TeachingGoldContractTest(unittest.TestCase):
    def test_every_quality_node_is_mandatory(self):
        for missing in sorted(TEACHING_TESTS):
            data = {"metadata": {"dbt_version": "1.9.8"}, "args": {"which": "build"}, "results": [
                {"unique_id": "model.api_gold.fct_teaching_progress", "status": "success"}
            ] + [{"unique_id": "test.api_gold." + n + (".0123456789" if n.startswith("not_null_") else ""),
                  "status": "pass", "failures": 0}
                 for n in TEACHING_TESTS if n != missing]}
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "run_results.json"
                path.write_text(json.dumps(data), encoding="utf-8")
                with self.assertRaises(RuntimeError):
                    verify_results(path, "education.teaching_progress")

    def test_teaching_output_identity_is_stable_and_separate_from_learning(self):
        teaching = "education.teaching_progress"
        first = identity(teaching, "manual__unit")
        self.assertEqual(first, identity(teaching, "manual__unit"))
        self.assertNotEqual(first[0], identity("education.learning_outcomes", "manual__unit")[0])
        self.assertEqual(outputs(teaching, "manual__unit"), {"teaching_progress_metrics": first[2]})
