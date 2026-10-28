from __future__ import annotations

import unittest
from unittest.mock import patch

from gx_runtime.api import CapturingStore, ERRORS, TableValidationApplication
from gx_runtime.errors import ValidationConfigurationError


class DelegateStore:
    def save(self, summary):
        return f"s3://bucket/{summary}"


class AirflowApiContractTest(unittest.TestCase):
    def test_runtime_routes_main_to_trino_and_candidate_to_spark(self):
        application = TableValidationApplication.__new__(TableValidationApplication)
        application.validator = object()
        application.candidate_validator = object()
        with patch.dict(
            "os.environ",
            {"GX_TRINO_NESSIE_BRANCH": "main", "GX_CANDIDATE_SPARK_ENABLED": "true"},
            clear=False,
        ):
            self.assertIs(application._validator_for_branch("main"), application.validator)
            self.assertIs(
                application._validator_for_branch("candidate_1"),
                application.candidate_validator,
            )

    def test_disabled_candidate_runtime_fails_closed(self):
        application = TableValidationApplication.__new__(TableValidationApplication)
        application.validator = object()
        application.candidate_validator = object()
        with patch.dict(
            "os.environ",
            {"GX_TRINO_NESSIE_BRANCH": "main", "GX_CANDIDATE_SPARK_ENABLED": "false"},
            clear=False,
        ), self.assertRaisesRegex(ValidationConfigurationError, "disabled"):
            application._validator_for_branch("candidate_1")

    def test_capturing_store_exposes_only_durable_result_uri(self):
        store = CapturingStore(DelegateStore())
        uri = store.save("validation-id")
        self.assertEqual(uri, "s3://bucket/validation-id")
        self.assertEqual(store.uri, uri)

    def test_required_error_categories_have_retry_policy(self):
        policies = {code: retryable for _, code, _, retryable in ERRORS}
        self.assertEqual(
            set(policies),
            {
                "DATA_QUALITY_FAILED",
                "GX_EXECUTION_ERROR",
                "GX_CONFIGURATION_ERROR",
                "SNAPSHOT_MISMATCH",
            },
        )
        self.assertFalse(policies["DATA_QUALITY_FAILED"])
        self.assertFalse(policies["GX_CONFIGURATION_ERROR"])
        self.assertTrue(policies["GX_EXECUTION_ERROR"])
        self.assertTrue(policies["SNAPSHOT_MISMATCH"])

    def test_malformed_request_is_configuration_error(self):
        application = TableValidationApplication.__new__(TableValidationApplication)
        with self.assertRaisesRegex(
            ValidationConfigurationError,
            "Invalid GX table validation request",
        ):
            application.validate({})


if __name__ == "__main__":
    unittest.main()
