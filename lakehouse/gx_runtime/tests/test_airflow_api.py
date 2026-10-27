from __future__ import annotations

import unittest

from gx_runtime.api import CapturingStore, ERRORS, TableValidationApplication
from gx_runtime.errors import ValidationConfigurationError


class DelegateStore:
    def save(self, summary):
        return f"s3://bucket/{summary}"


class AirflowApiContractTest(unittest.TestCase):
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
        with self.assertRaisesRegex(ValidationConfigurationError, "missing"):
            application.validate({})


if __name__ == "__main__":
    unittest.main()
