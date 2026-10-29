from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from gx_runtime.bronze import validate_bronze_evidence
from gx_runtime.contracts import ContractRegistry
from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.suite_registry import SuiteRegistry
from gx_runtime.validation_registry import DatasetValidationRegistry


ROOT = Path(__file__).resolve().parents[2] / "gx"


def evidence(**metrics):
    values = {
        "readable": True,
        "row_count": 10,
        "corrupt_record_ratio": 0.0,
        "metadata_complete": True,
        "checksum_present": True,
        "duplicate_ingestion_count": 0,
        "format_supported": True,
        "header_valid": True,
    }
    values.update(metrics)
    return {
        "dataset_id": "education.teaching_progress",
        "contract_version": "1.0.0",
        "source_kind": "file",
        "run_id": "manual__bronze_1",
        "artifact_ref": "s3://quality/bronze/file.csv",
        "metrics": values,
    }


class BronzeGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.suites = SuiteRegistry(ROOT / "suite_registry.json")
        cls.routes = DatasetValidationRegistry(
            ROOT / "validation_registry.json",
            contract_registry=ContractRegistry(ROOT / "contract_registry.json"),
            suite_registry=cls.suites,
        )

    def test_registered_file_evidence_passes_technical_gate(self):
        result = validate_bronze_evidence(
            evidence(), routes=self.routes, suites=self.suites
        )
        self.assertEqual(result.status, "PASSED")
        self.assertEqual(result.data_asset_ref, "lakehouse.bronze.teaching_progress")
        self.assertTrue(result.merge_allowed)
        self.assertEqual(result.evaluated_expectations, 8)

    def test_corrupt_or_empty_bronze_is_blocked(self):
        result = validate_bronze_evidence(
            evidence(row_count=0, corrupt_record_ratio=1.0),
            routes=self.routes,
            suites=self.suites,
        )
        self.assertEqual(result.status, "FAILED")
        self.assertFalse(result.merge_allowed)
        self.assertEqual(len(result.failures), 2)

    def test_missing_required_source_metric_fails_closed(self):
        payload = evidence()
        del payload["metrics"]["header_valid"]
        with self.assertRaisesRegex(ValidationConfigurationError, "header_valid"):
            validate_bronze_evidence(payload, routes=self.routes, suites=self.suites)

    def test_api_and_database_use_source_specific_expectations(self):
        common = evidence()["metrics"]
        common.pop("format_supported")
        common.pop("header_valid")
        api = evidence()
        api["source_kind"] = "api"
        api["metrics"] = {**common, "http_success": True, "pagination_complete": True}
        database = evidence()
        database["source_kind"] = "database"
        database["metrics"] = {
            **common,
            "extraction_complete": True,
            "watermark_present": True,
        }
        self.assertTrue(
            validate_bronze_evidence(api, routes=self.routes, suites=self.suites).merge_allowed
        )
        self.assertTrue(
            validate_bronze_evidence(
                database, routes=self.routes, suites=self.suites
            ).merge_allowed
        )


if __name__ == "__main__":
    unittest.main()
