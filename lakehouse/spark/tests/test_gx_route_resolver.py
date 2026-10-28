from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from gx_route_resolver import (
    GXRouteConfigurationError,
    GXRouteNotFoundError,
    resolve_route,
    resolve_route_or_baseline,
)


class GXRouteResolverTest(unittest.TestCase):
    def _root(self, *, contracts, routes):
        temporary = tempfile.TemporaryDirectory(dir=Path.cwd())
        root = Path(temporary.name)
        (root / "contract_registry.json").write_text(
            json.dumps({"contracts": contracts}), encoding="utf-8"
        )
        (root / "validation_registry.json").write_text(
            json.dumps({"routes": routes}), encoding="utf-8"
        )
        return temporary, root

    def test_new_dataset_is_discovered_from_registry_without_python_changes(self):
        contracts = [{
            "dataset_id": "finance.transactions",
            "contract_version": "1.0.0",
            "effective_from": "2026-01-01",
            "definition": "unused-by-airflow.json",
        }]
        routes = [{
            "dataset_id": "finance.transactions",
            "layer": "silver",
            "contract_version": "1.0.0",
            "data_asset_ref": "lakehouse.silver.finance_transactions",
            "supported_source_kinds": ["file", "api", "database"],
            "suites": {"sql": [{"name": "finance_core", "version": "1.0.0"}]},
        }]
        temporary, root = self._root(contracts=contracts, routes=routes)

        with temporary:
            plan = resolve_route(
                dataset_id="finance.transactions",
                data_layer="silver",
                source_kind="file",
                execution_engine="sql",
                config_root=root,
                as_of=date(2026, 10, 28),
            )

        self.assertEqual(plan.contract_version, "1.0.0")
        self.assertEqual(plan.data_asset_ref, "lakehouse.silver.finance_transactions")

    def test_latest_effective_version_is_selected(self):
        contracts = [
            {"dataset_id": "example.data", "contract_version": version,
             "effective_from": effective, "definition": "unused.json"}
            for version, effective in (
                ("1.0.0", "2026-01-01"),
                ("1.5.0", "2026-09-01"),
                ("2.0.0", "2027-01-01"),
            )
        ]
        routes = [
            {"dataset_id": "example.data", "layer": "silver",
             "contract_version": version,
             "data_asset_ref": f"lakehouse.silver.example_{version.replace('.', '_')}",
             "supported_source_kinds": ["file"],
             "suites": {"sql": [{"name": "suite", "version": version}]}}
            for version in ("1.0.0", "1.5.0", "2.0.0")
        ]
        temporary, root = self._root(contracts=contracts, routes=routes)

        with temporary:
            plan = resolve_route(
                dataset_id="example.data", data_layer="silver",
                source_kind="file", execution_engine="sql",
                config_root=root, as_of=date(2026, 10, 28),
            )

        self.assertEqual(plan.contract_version, "1.5.0")

    def test_unsupported_source_or_engine_has_no_route(self):
        contracts = [{
            "dataset_id": "example.data", "contract_version": "1.0.0",
            "effective_from": "2026-01-01", "definition": "unused.json",
        }]
        routes = [{
            "dataset_id": "example.data", "layer": "silver",
            "contract_version": "1.0.0", "data_asset_ref": "lakehouse.silver.example",
            "supported_source_kinds": ["api"],
            "suites": {"sql": [{"name": "suite", "version": "1.0.0"}]},
        }]
        temporary, root = self._root(contracts=contracts, routes=routes)

        with temporary, self.assertRaises(GXRouteNotFoundError):
            resolve_route(
                dataset_id="example.data", data_layer="silver",
                source_kind="file", execution_engine="sql",
                config_root=root, as_of=date(2026, 10, 28),
            )

    def test_unknown_dataset_receives_shared_baseline_plan(self):
        temporary, root = self._root(contracts=[], routes=[])

        with temporary:
            plan = resolve_route_or_baseline(
                dataset_id=None,
                data_asset_ref="lakehouse.silver.dynamic_students",
                source_kind="file",
                config_root=root,
                as_of=date(2026, 10, 28),
            )

        self.assertEqual(plan.coverage, "BASELINE")
        self.assertEqual(plan.contract_status, "MISSING")
        self.assertEqual(plan.dataset_id, "dynamic.dynamic_students")

    def test_baseline_rejects_asset_outside_silver_namespace(self):
        temporary, root = self._root(contracts=[], routes=[])

        with temporary, self.assertRaises(GXRouteConfigurationError):
            resolve_route_or_baseline(
                dataset_id=None,
                data_asset_ref="lakehouse.gold.unsafe",
                source_kind="file",
                config_root=root,
                as_of=date(2026, 10, 28),
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
