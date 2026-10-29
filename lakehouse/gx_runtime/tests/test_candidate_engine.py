from __future__ import annotations

import unittest
from dataclasses import replace
from pathlib import Path

from gx_runtime.candidate_engine import CandidateSparkSqlSuiteExecutor
from gx_runtime.errors import ValidationExecutionError
from gx_runtime.models import (
    ExecutionEngine,
    ExecutionMode,
    SourceKind,
    SuiteReference,
    ValidationRequest,
)
from gx_runtime.suite_registry import SuiteRegistry


CONFIG = Path(__file__).resolve().parents[2] / "gx"


class FakeFrame:
    columns = (
        "ma_ban_ghi", "ma_don_vi", "ten_don_vi", "ma_lop_hoc_phan",
        "nam_hoc", "hoc_ky", "ty_le_tien_do_giang_day",
        "thoi_gian_cap_nhat_nguon", "_source_system", "_source_type",
        "_dataset", "_schema_version", "_ingestion_mode", "_batch_id",
        "_source_updated_at", "_ingested_at", "_record_checksum",
        "_silver_updated_at",
    )


class FakeQuery:
    def __init__(self, value):
        self.value = value

    def first(self):
        return (self.value,)


class FakeSpark:
    def __init__(self, observations):
        self.observations = iter(observations)
        self.queries = []
        self.tables = []

    def table(self, name):
        self.tables.append(name)
        return FakeFrame()

    def sql(self, query):
        self.queries.append(query)
        return FakeQuery(next(self.observations))


def request(branch="candidate_1"):
    return ValidationRequest(
        data_asset_ref="lakehouse.silver.teaching_progress",
        dataset_id="education.teaching_progress",
        data_layer="silver",
        execution_engine=ExecutionEngine.SQL,
        contract_version="1.0.0",
        suites=(SuiteReference("teaching_progress_silver_core", "1.0.0"),),
        run_id="manual__candidate",
        execution_mode=ExecutionMode.ENFORCE,
        nessie_branch=branch,
        snapshot_id="7",
        nessie_commit_hash="commit-7",
        source_kind=SourceKind.API,
    )


class CandidateSparkSqlSuiteExecutorTest(unittest.TestCase):
    def test_reconciliation_query_targets_gold_alias_and_candidate_silver(self):
        digest = "0123456789abcdef01234567"
        candidate_request = replace(
            request(),
            data_asset_ref=f"lakehouse.gold.teaching_progress_metrics_{digest}",
        )
        value = CandidateSparkSqlSuiteExecutor._candidate_query(
            "SELECT * FROM lakehouse.gold.teaching_progress_metrics "
            "JOIN lakehouse.silver.teaching_progress USING (ma_ban_ghi)",
            candidate_request,
            "gx_candidate_1",
        )

        self.assertIn(
            f"gx_candidate_1.gold.teaching_progress_metrics_{digest}", value
        )
        self.assertIn("gx_candidate_1.silver.teaching_progress", value)
        self.assertNotIn("lakehouse.gold", value)
        self.assertNotIn("lakehouse.silver", value)

    def test_executes_sql_suite_on_request_scoped_catalog(self):
        spark = FakeSpark((0.0, 0, 4, 1, 0))
        executor = CandidateSparkSqlSuiteExecutor(
            SuiteRegistry(CONFIG / "suite_registry.json"),
            lambda branch: (spark, "gx_candidate_1"),
        )

        result = executor(request())

        self.assertTrue(result.success)
        self.assertEqual(result.evaluated_expectations, 6)
        self.assertEqual(result.successful_expectations, 6)
        self.assertEqual(spark.tables, ["gx_candidate_1.silver.teaching_progress"])
        self.assertTrue(all("lakehouse.silver.teaching_progress" not in q for q in spark.queries))
        self.assertTrue(any("timestampdiff(HOUR" in q for q in spark.queries))

    def test_rejects_main_to_prevent_bypassing_trino_boundary(self):
        executor = CandidateSparkSqlSuiteExecutor(
            SuiteRegistry(CONFIG / "suite_registry.json"),
            lambda branch: (FakeSpark(()), "gx_main"),
        )
        with self.assertRaisesRegex(ValidationExecutionError, "non-main"):
            executor(request(branch="main"))

    def test_blocking_observation_is_reported(self):
        spark = FakeSpark((0.5, 0, 4, 1, 0))
        executor = CandidateSparkSqlSuiteExecutor(
            SuiteRegistry(CONFIG / "suite_registry.json"),
            lambda branch: (spark, "gx_candidate_1"),
        )
        result = executor(request())
        self.assertFalse(result.success)
        self.assertEqual(result.failures[0].expectation_type, "null_ratio")


if __name__ == "__main__":
    unittest.main(verbosity=2)
