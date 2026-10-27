from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gx_runtime.contracts import ContractRegistry, load_contract
from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.models import ExecutionEngine


CONTRACT_PATH = Path("/opt/gx/config/contracts/education.teaching_progress/1.0.0.json")
if not CONTRACT_PATH.exists():
    CONTRACT_PATH = Path("lakehouse/gx/contracts/education.teaching_progress/1.0.0.json")
REGISTRY_PATH = CONTRACT_PATH.parents[2] / "contract_registry.json"


class TeachingProgressContractTest(unittest.TestCase):
    def test_registry_resolves_exact_contract_version(self):
        contract = ContractRegistry(REGISTRY_PATH).resolve(
            "education.teaching_progress", "1.0.0"
        )

        self.assertEqual(contract.dataset_id, "education.teaching_progress")

    def test_contract_loads_with_all_required_rule_families(self):
        contract = load_contract(CONTRACT_PATH)

        self.assertEqual(contract.dataset_id, "education.teaching_progress")
        self.assertEqual(contract.contract_version, "1.0.0")
        self.assertEqual(
            {rule.rule_type for rule in contract.rules},
            {
                "schema",
                "freshness",
                "null_ratio",
                "duplicate_ratio",
                "volume",
                "range",
                "row_count_match",
                "key_set_match",
                "column_reconciliation",
            },
        )

    def test_every_rule_declares_engine_compatibility(self):
        contract = load_contract(CONTRACT_PATH)

        self.assertTrue(all(rule.supported_engines for rule in contract.rules))
        self.assertTrue(contract.rules_for_engine(ExecutionEngine.SPARK))
        self.assertTrue(contract.rules_for_engine(ExecutionEngine.SQL))

    def test_unverified_candidate_uniqueness_is_not_critical(self):
        contract = load_contract(CONTRACT_PATH)
        uniqueness = [
            rule for rule in contract.rules if rule.rule_type == "duplicate_ratio"
        ]

        self.assertTrue(uniqueness)
        self.assertTrue(all(rule.severity == "warning" for rule in uniqueness))
        self.assertFalse(contract.payload["grain"]["business_semantics_verified"])

    def test_schema_evolution_policy_is_explicit(self):
        policy = load_contract(CONTRACT_PATH).payload["schema_evolution"]

        self.assertEqual(policy["additional_columns"], "allow_with_warning")
        self.assertEqual(policy["column_removal"], "forbid")
        self.assertEqual(policy["type_change"], "forbid")

    def test_loader_rejects_critical_uniqueness_before_business_verification(self):
        payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        payload["layers"]["silver"]["rules"][2]["severity"] = "critical"
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "invalid-contract.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                ValidationConfigurationError, "business-verified grain"
            ):
                load_contract(path)

    def test_ratio_threshold_must_be_bounded(self):
        payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        payload["layers"]["silver"]["rules"][1]["threshold"]["max_ratio"] = 1.1
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "invalid-ratio.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValidationConfigurationError, "between 0 and 1"):
                load_contract(path)


if __name__ == "__main__":
    unittest.main()
