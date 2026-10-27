"""Reject incomplete, skipped, duplicate or non-build dbt receipts."""
import json
import hashlib
import re
from pathlib import Path


LEARNING_TESTS = {
    "learning_outcomes_grain", "learning_outcomes_periods",
    "learning_outcomes_reconciliation", "learning_outcomes_row_checks",
    "learning_outcomes_schema", "learning_period_grain",
    "learning_period_reconciliation", "learning_period_row_checks", "learning_period_schema",
} | {"learning_outcomes_lineage_part_" + str(i) for i in range(4)}
TEACHING_TESTS = {
    "teaching_progress_grain", "teaching_progress_lineage", "teaching_progress_monitor_count",
    "teaching_progress_range", "teaching_progress_reconciliation", "teaching_progress_schema",
} | {"not_null_fct_teaching_progress_" + col for col in (
    "ma_lop_hoc_phan", "nam_hoc", "hoc_ky", "ty_le_tien_do_giang_day", "source_record_id",
    "source_updated_at", "source_batch_id", "source_record_checksum", "_gold_updated_at",
)}
CONTRACTS = {
    "education.learning_outcomes": (
        {"fct_learning_outcomes", "agg_learning_outcomes_by_period"}, LEARNING_TESTS),
    "education.teaching_progress": ({"fct_teaching_progress"}, TEACHING_TESTS),
}
GENERIC_TESTS = {name for name in TEACHING_TESTS if name.startswith("not_null_")}


def logical_id(unique_id):
    if not isinstance(unique_id, str):
        raise RuntimeError("Invalid dbt node ID")
    parts = unique_id.split(".")
    if len(parts) == 4 and parts[:2] == ["test", "api_gold"] and parts[2] in GENERIC_TESTS:
        if not re.fullmatch(r"[0-9a-f]{10}", parts[3]):
            raise RuntimeError("Invalid generic-test hash suffix")
        return ".".join(parts[:3])
    if len(parts) != 3 or (parts[:2] == ["test", "api_gold"] and parts[2] in GENERIC_TESTS):
        raise RuntimeError("Invalid or missing dbt node ID suffix")
    return unique_id


def verify_results(path, dataset):
    if dataset not in CONTRACTS:
        raise ValueError("Unsupported result contract")
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if data.get("args", {}).get("which") != "build":
        raise RuntimeError("Only dbt build artifacts are accepted")
    if data.get("metadata", {}).get("dbt_version") != "1.9.8":
        raise RuntimeError("Unexpected dbt artifact version")
    model_names, test_names = CONTRACTS[dataset]
    expected = {"model.api_gold." + n for n in model_names} | {"test.api_gold." + n for n in test_names}
    rows = data.get("results")
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise RuntimeError("Missing dbt results")
    # dbt generic tests append a hash. Count logical rules, not suffix variants:
    # two hashes for one rule cannot stand in for a missing required rule.
    ids = [logical_id(r.get("unique_id")) for r in rows]
    if len(ids) != len(expected) or set(ids) != expected:
        raise RuntimeError("Incomplete, duplicate or unexpected dbt nodes")
    for row in rows:
        test = row["unique_id"].startswith("test.")
        if row.get("status") != ("pass" if test else "success"):
            raise RuntimeError("A dbt model/test failed, warned or was skipped")
        if test and (type(row.get("failures")) is not int or row["failures"] != 0):
            raise RuntimeError("Missing or nonzero data-test failure count")
    return {"models": len(model_names), "tests": len(test_names), "command": "build"}


def verify_sealed_results(path, dataset, expected_sha256):
    if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected_sha256:
        raise RuntimeError("Sealed build artifact changed")
    return verify_results(path, dataset)
