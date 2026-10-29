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
                "GX_CALLER_CREDENTIALS": (
                    '{"airflow":"airflow-unit-test-token-that-is-at-least-32-chars",'
                    '"release-controller":["release-current-token-that-is-at-least-32-chars",'
                    '"release-next-token-that-is-at-least-32-chars"]}'
                ),
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
                "Authorization": "Bearer airflow-unit-test-token-that-is-at-least-32-chars",
                "X-GX-Caller": "airflow",
            }
        )
        self.assertEqual(caller, "airflow")
        self.assertEqual(policy.max_request_bytes, 4096)
        policy.authorize_governance("release-controller")
        with self.assertRaises(GXAuthorizationError):
            policy.authorize_governance("airflow")

    def test_invalid_token_and_spoofed_caller_are_rejected(self):
        policy = HTTPBoundarySecurity.from_env()
        with self.assertRaises(GXAuthenticationError):
            policy.authenticate(
                {"Authorization": "Bearer wrong", "X-GX-Caller": "airflow"}
            )
        with self.assertRaises(GXAuthorizationError):
            policy.authenticate(
                {
                    "Authorization": "Bearer airflow-unit-test-token-that-is-at-least-32-chars",
                    "X-GX-Caller": "release-controller",
                }
            )

    def test_rotated_token_authenticates_same_caller(self):
        policy = HTTPBoundarySecurity.from_env()
        caller = policy.authenticate(
            {"Authorization": "Bearer release-next-token-that-is-at-least-32-chars"}
        )
        self.assertEqual(caller, "release-controller")

    def test_expired_caller_credential_is_rejected(self):
        with patch.dict(
            os.environ,
            {
                "GX_CALLER_CREDENTIALS": (
                    '{"airflow":{"token":"expired-token-that-is-at-least-thirty-two-characters",'
                    '"expires_at":"2020-01-01T00:00:00Z"},'
                    '"release-controller":"release-current-token-that-is-at-least-32-chars"}'
                )
            },
        ):
            policy = HTTPBoundarySecurity.from_env()
            with self.assertRaises(GXAuthenticationError):
                policy.authenticate(
                    {"Authorization": "Bearer expired-token-that-is-at-least-thirty-two-characters"}
                )

    def test_production_requires_expiring_credentials(self):
        with patch.dict(os.environ, {"GX_ENVIRONMENT": "production"}), self.assertRaisesRegex(
            ValidationConfigurationError, "expires_at"
        ):
            HTTPBoundarySecurity.from_env()

    def test_short_or_shared_token_and_unsafe_limits_are_configuration_errors(self):
        with patch.dict(os.environ, {"GX_CALLER_CREDENTIALS": '{"airflow":"short"}'}), self.assertRaises(
            ValidationConfigurationError
        ):
            HTTPBoundarySecurity.from_env()
        shared = "same-token-that-is-longer-than-thirty-two-characters"
        with patch.dict(
            os.environ,
            {"GX_CALLER_CREDENTIALS": '{"a":"%s","b":"%s"}' % (shared, shared)},
        ), self.assertRaisesRegex(ValidationConfigurationError, "unique"):
            HTTPBoundarySecurity.from_env()
        with patch.dict(os.environ, {"GX_MAX_REQUEST_BYTES": "9999999"}), self.assertRaises(
            ValidationConfigurationError
        ):
            HTTPBoundarySecurity.from_env()


if __name__ == "__main__":
    unittest.main()
