from __future__ import annotations

import unittest

from gx_validation_request import (
    GXTableValidationRequest,
    GXValidationRequestError,
)


def request(**overrides):
    values = {
        "dataset_id": "education.teaching_progress",
        "data_layer": "silver",
        "source_kind": "file",
        "run_id": "manual__42",
        "data_asset_ref": "lakehouse.silver.teaching_progress",
        "nessie_branch": "candidate_42",
        "snapshot_id": "123",
        "nessie_commit_hash": "abc",
        "contract_version": "1.0.0",
    }
    values.update(overrides)
    return GXTableValidationRequest(**values)


class GXTableValidationRequestTest(unittest.TestCase):
    def test_round_trip_preserves_the_complete_table_identity(self):
        original = request()
        restored = GXTableValidationRequest.from_mapping(original.as_payload())

        self.assertEqual(restored, original)
        self.assertEqual(restored.nessie_branch, "candidate_42")
        self.assertEqual(restored.snapshot_id, "123")
        self.assertEqual(restored.nessie_commit_hash, "abc")

    def test_missing_snapshot_identity_is_rejected_before_http(self):
        for field in ("nessie_branch", "snapshot_id", "nessie_commit_hash"):
            with self.subTest(field=field), self.assertRaises(
                GXValidationRequestError
            ):
                request(**{field: ""})

    def test_unsafe_asset_and_unknown_enums_are_rejected(self):
        invalid = (
            {"data_asset_ref": "lakehouse.silver.x; DROP TABLE y"},
            {"source_kind": "ftp"},
            {"data_layer": "bronze"},
            {"execution_mode": "maybe"},
        )
        for overrides in invalid:
            with self.subTest(overrides=overrides), self.assertRaises(
                GXValidationRequestError
            ):
                request(**overrides)

    def test_baseline_contract_and_layer_must_be_consistent(self):
        baseline = request(
            dataset_id="dynamic.students",
            coverage="baseline",
            contract_version="baseline-1.0.0",
            data_asset_ref="lakehouse.silver.dynamic_students",
        )
        self.assertEqual(baseline.coverage, "BASELINE")

        with self.assertRaises(GXValidationRequestError):
            request(coverage="BASELINE", contract_version="1.0.0")
        with self.assertRaises(GXValidationRequestError):
            request(
                coverage="BASELINE",
                contract_version="baseline-1.0.0",
                data_layer="gold",
                data_asset_ref="lakehouse.gold.dynamic_students",
            )

    def test_contract_version_may_be_omitted_for_dynamic_registry_resolution(self):
        value = request(contract_version=None)
        self.assertIsNone(value.as_payload()["contract_version"])

    def test_physical_asset_round_trip_preserves_canonical_contract_identity(self):
        physical = "lakehouse.gold.metrics_0123456789abcdef01234567"
        original = request(
            data_layer="gold",
            data_asset_ref="lakehouse.gold.metrics",
            physical_data_asset_ref=physical,
        )
        restored = GXTableValidationRequest.from_mapping(original.as_payload())

        self.assertEqual(restored, original)
        self.assertEqual(restored.data_asset_ref, "lakehouse.gold.metrics")
        self.assertEqual(restored.execution_asset_ref, physical)
        self.assertEqual(request().execution_asset_ref, request().data_asset_ref)

    def test_unsafe_physical_asset_is_rejected(self):
        with self.assertRaises(GXValidationRequestError):
            request(physical_data_asset_ref="lakehouse.gold.x; DROP TABLE y")


if __name__ == "__main__":
    unittest.main(verbosity=2)
