from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.http_security import (
    GXAuthenticationError,
    GXAuthorizationError,
    HTTPBoundarySecurity,
)


class HTTPBoundarySecurityTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(
            os.environ,
            {
                "GX_SERVICE_TOKEN": "unit-test-service-token-that-is-at-least-32-chars",
                "GX_ALLOWED_CALLERS": "airflow,release-controller",
                "GX_GOVERNANCE_CALLERS": "release-controller",
                "GX_MAX_REQUEST_BYTES": "4096",
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_valid_bearer_and_allowed_caller_are_accepted(self):
        policy = HTTPBoundarySecurity.from_env()
        caller = policy.authenticate(
            {
                "Authorization": "Bearer unit-test-service-token-that-is-at-least-32-chars",
                "X-GX-Caller": "airflow",
            }
        )
        self.assertEqual(caller, "airflow")
        self.assertEqual(policy.max_request_bytes, 4096)
        policy.authorize_governance("release-controller")
        with self.assertRaises(GXAuthorizationError):
            policy.authorize_governance("airflow")

    def test_invalid_token_and_unknown_caller_are_rejected(self):
        policy = HTTPBoundarySecurity.from_env()
        with self.assertRaises(GXAuthenticationError):
            policy.authenticate(
                {"Authorization": "Bearer wrong", "X-GX-Caller": "airflow"}
            )
        with self.assertRaises(GXAuthorizationError):
            policy.authenticate(
                {
                    "Authorization": "Bearer unit-test-service-token-that-is-at-least-32-chars",
                    "X-GX-Caller": "unknown",
                }
            )

    def test_short_token_and_unsafe_limits_are_configuration_errors(self):
        with patch.dict(os.environ, {"GX_SERVICE_TOKEN": "short"}), self.assertRaises(
            ValidationConfigurationError
        ):
            HTTPBoundarySecurity.from_env()
        with patch.dict(os.environ, {"GX_MAX_REQUEST_BYTES": "9999999"}), self.assertRaises(
            ValidationConfigurationError
        ):
            HTTPBoundarySecurity.from_env()


if __name__ == "__main__":
    unittest.main()
