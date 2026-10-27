from __future__ import annotations

import unittest
from pathlib import Path

from gx_runtime.contracts import ContractRegistry
from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.models import ExecutionEngine, SourceKind
from gx_runtime.policy import ValidationPolicy
from gx_runtime.models import EngineValidationResult
from gx_runtime.suite_registry import SuiteRegistry
from gx_runtime.validation_registry import DatasetValidationRegistry


GX_ROOT = Path("/opt/gx/config")
if not GX_ROOT.exists():
    GX_ROOT = Path(__file__).resolve().parents[2] / "gx"


def registry():
    return DatasetValidationRegistry(
        GX_ROOT / "validation_registry.json",
        contract_registry=ContractRegistry(GX_ROOT / "contract_registry.json"),
        suite_registry=SuiteRegistry(GX_ROOT / "suite_registry.json"),
    )


class MultiDatasetRoutingTest(unittest.TestCase):
    def test_two_datasets_resolve_by_dataset_layer_and_contract(self):
        routes = registry()
        teaching = routes.resolve(
            dataset_id="education.teaching_progress",
            data_layer="silver",
            contract_version="1.0.0",
            execution_engine=ExecutionEngine.SQL,
            source_kind=SourceKind.API,
        )
        learning = routes.resolve(
            dataset_id="education.learning_outcomes",
            data_layer="silver",
            contract_version="1.0.0",
            execution_engine=ExecutionEngine.SQL,
            source_kind=SourceKind.API,
        )

        self.assertEqual(teaching.suites[0].name, "teaching_progress_silver_core")
        self.assertEqual(learning.suites[0].name, "learning_outcomes_silver_core")
        self.assertNotEqual(teaching.data_asset_ref, learning.data_asset_ref)

    def test_file_api_and_database_reuse_the_exact_same_suite(self):
        routes = registry()
        plans = [
            routes.resolve(
                dataset_id="education.learning_outcomes",
                data_layer="gold",
                contract_version="1.0.0",
                execution_engine=ExecutionEngine.SQL,
                source_kind=source_kind,
            )
            for source_kind in (
                SourceKind.FILE,
                SourceKind.API,
                SourceKind.DATABASE,
            )
        ]

        self.assertEqual(len({plan.suites for plan in plans}), 1)
        self.assertEqual(len({plan.contract_version for plan in plans}), 1)
        self.assertEqual(len({plan.data_asset_ref for plan in plans}), 1)
        self.assertEqual(
            {plan.source_kind for plan in plans},
            {SourceKind.FILE, SourceKind.API, SourceKind.DATABASE},
        )

    def test_source_kind_is_traceability_not_a_routing_key(self):
        plan = registry().resolve(
            dataset_id="education.learning_outcomes",
            data_layer="silver",
            contract_version="1.0.0",
            execution_engine=ExecutionEngine.SQL,
            source_kind=SourceKind.DATABASE,
        )
        request = plan.request(
            run_id="phase7-database",
            nessie_branch="candidate",
            snapshot_id="123",
            nessie_commit_hash="abc",
        )
        summary = ValidationPolicy().apply(
            request,
            EngineValidationResult(True, 1, 1),
        )

        self.assertEqual(request.source_kind, SourceKind.DATABASE)
        self.assertEqual(summary.as_dict()["source_kind"], "database")
        self.assertEqual(request.suites, plan.suites)

    def test_streaming_is_explicitly_disabled_until_window_policy_exists(self):
        with self.assertRaisesRegex(ValidationConfigurationError, "not enabled"):
            registry().resolve(
                dataset_id="education.learning_outcomes",
                data_layer="silver",
                contract_version="1.0.0",
                execution_engine=ExecutionEngine.SQL,
                source_kind=SourceKind.STREAMING,
            )

    def test_unknown_contract_and_wrong_engine_fail_closed(self):
        routes = registry()
        with self.assertRaisesRegex(ValidationConfigurationError, "No validation route"):
            routes.resolve(
                dataset_id="education.learning_outcomes",
                data_layer="silver",
                contract_version="2.0.0",
                execution_engine=ExecutionEngine.SQL,
                source_kind=SourceKind.API,
            )
        with self.assertRaisesRegex(ValidationConfigurationError, "Engine spark"):
            routes.resolve(
                dataset_id="education.learning_outcomes",
                data_layer="silver",
                contract_version="1.0.0",
                execution_engine=ExecutionEngine.SPARK,
                source_kind=SourceKind.API,
            )


if __name__ == "__main__":
    unittest.main()
