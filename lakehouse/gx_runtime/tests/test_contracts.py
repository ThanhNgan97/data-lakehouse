from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from unittest.mock import patch

from gx_runtime.contracts import (
    ContractRegistry,
    load_contract,
    validate_contract_compatibility,
)
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

    def test_pilot_contract_can_be_disabled_for_governed_runtime(self):
        registry = ContractRegistry(REGISTRY_PATH)
        with patch.dict("os.environ", {"GX_ALLOW_PILOT_CONTRACTS": "false"}):
            with self.assertRaisesRegex(ValidationConfigurationError, "not active"):
                registry.resolve("education.teaching_progress", "1.0.0")

    def test_active_registration_requires_approval_evidence(self):
        payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        payload["status"] = "active"
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            definition = root / "contract.json"
            definition.write_text(json.dumps(payload), encoding="utf-8")
            registry = root / "contract_registry.json"
            registry.write_text(
                json.dumps(
                    {
                        "contracts": [
                            {
                                "dataset_id": payload["dataset_id"],
                                "contract_version": payload["contract_version"],
                                "status": "active",
                                "owner": payload["owner"],
                                "effective_from": "2026-01-01",
                                "definition": "contract.json",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValidationConfigurationError, "requires approved"):
                ContractRegistry(registry)

    def test_breaking_change_requires_new_major_version(self):
        old_payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        new_payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        new_payload["contract_version"] = "1.1.0"
        del new_payload["layers"]["silver"]["columns"]["ma_ban_ghi"]
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            root = Path(directory)
            old_path = root / "old.json"
            new_path = root / "new.json"
            old_path.write_text(json.dumps(old_payload), encoding="utf-8")
            new_path.write_text(json.dumps(new_payload), encoding="utf-8")
            with self.assertRaisesRegex(ValidationConfigurationError, "new major"):
                validate_contract_compatibility(
                    load_contract(old_path), load_contract(new_path)
                )


if __name__ == "__main__":
    unittest.main()
