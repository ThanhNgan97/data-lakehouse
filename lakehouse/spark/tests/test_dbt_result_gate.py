import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, os.environ.get("DBT_SERVICE_DIR", str(Path(__file__).resolve().parents[2] / "dbt/service")))
from result_gate import CONTRACTS, verify_results, verify_sealed_results


def receipt(dataset):
    models, tests = CONTRACTS[dataset]
    return {"metadata": {"dbt_version": "1.9.8"}, "args": {"which": "build"}, "results": [
        {"unique_id": "model.api_gold." + n, "status": "success"} for n in sorted(models)
    ] + [{"unique_id": "test.api_gold." + n + (".0123456789" if n.startswith("not_null_") else ""),
          "status": "pass", "failures": 0} for n in sorted(tests)]}


class ResultGateTest(unittest.TestCase):
    def test_generic_hash_variants_cannot_replace_another_required_rule(self):
        dataset = "education.teaching_progress"
        data = receipt(dataset)
        generic = [r for r in data["results"] if ".not_null_" in r["unique_id"]]
        generic[1]["unique_id"] = generic[0]["unique_id"].rsplit(".", 1)[0] + ".abcdef0123"
        with self.assertRaises(RuntimeError):
            self.verify(data, dataset)

    def test_invalid_missing_or_extra_generic_suffix_is_refused(self):
        dataset = "education.teaching_progress"
        for suffix in ["", ".abcdef", ".01234567890", ".ZZZZZZZZZZ", ".0123456789.extra"]:
            data = receipt(dataset)
            row = next(r for r in data["results"] if ".not_null_" in r["unique_id"])
            row["unique_id"] = row["unique_id"].rsplit(".", 1)[0] + suffix
            with self.subTest(suffix=suffix), self.assertRaises(RuntimeError):
                self.verify(data, dataset)

    def test_suffix_on_singular_test_or_model_is_refused(self):
        for index in [0, -1]:
            data = receipt("education.learning_outcomes")
            data["results"][index]["unique_id"] += ".0123456789"
            with self.subTest(index=index), self.assertRaises(RuntimeError):
                self.verify(data)

    def test_valid_but_modified_receipt_breaks_seal(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "run_results.json"
            data = receipt("education.learning_outcomes")
            path.write_text(json.dumps(data), encoding="utf-8")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            verify_sealed_results(path, "education.learning_outcomes", digest)
            data["elapsed_time"] = 123
            path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                verify_sealed_results(path, "education.learning_outcomes", digest)

    def verify(self, data, dataset="education.learning_outcomes"):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "run_results.json"
            path.write_text(json.dumps(data), encoding="utf-8")
            return verify_results(path, dataset)

    def test_both_complete_builds(self):
        for dataset, count in [("education.learning_outcomes", 13), ("education.teaching_progress", 15)]:
            with self.subTest(dataset=dataset):
                self.assertEqual(self.verify(receipt(dataset), dataset)["tests"], count)

    def test_compile_or_run_receipt_is_not_a_build(self):
        for command in ["compile", "parse", "run", "test", None]:
            data = receipt("education.learning_outcomes")
            data["args"]["which"] = command
            with self.subTest(command=command), self.assertRaises(RuntimeError):
                self.verify(data)

    def test_each_missing_lineage_part_blocks_publication(self):
        for part in range(4):
            data = receipt("education.learning_outcomes")
            data["results"] = [r for r in data["results"] if not r["unique_id"].endswith("lineage_part_" + str(part))]
            with self.subTest(part=part), self.assertRaises(RuntimeError):
                self.verify(data)

    def test_duplicate_cannot_replace_missing_node(self):
        data = receipt("education.learning_outcomes")
        data["results"][-1] = copy.deepcopy(data["results"][-2])
        with self.assertRaises(RuntimeError):
            self.verify(data)

    def test_failed_skipped_warned_or_nonzero_test_is_refused(self):
        for status, failures in [("fail", 1), ("skipped", None), ("warn", 0), ("pass", 1), ("pass", None)]:
            data = receipt("education.learning_outcomes")
            data["results"][-1].update(status=status, failures=failures)
            with self.subTest(status=status, failures=failures), self.assertRaises(RuntimeError):
                self.verify(data)

    def test_missing_mart_and_wrong_version_are_refused(self):
        data = receipt("education.learning_outcomes")
        data["results"] = [r for r in data["results"] if "agg_learning_outcomes_by_period" not in r["unique_id"]]
        with self.assertRaises(RuntimeError):
            self.verify(data)
        data = receipt("education.learning_outcomes")
        data["metadata"]["dbt_version"] = "unknown"
        with self.assertRaises(RuntimeError):
            self.verify(data)
