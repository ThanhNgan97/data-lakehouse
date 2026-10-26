from __future__ import annotations

import json
import os
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

from gx_runtime import smoke


class RuntimeConfigurationTest(unittest.TestCase):
    def test_trino_url_uses_environment(self):
        with patch.dict(
            os.environ,
            {
                "TRINO_HOST": "query.example",
                "TRINO_PORT": "8443",
                "TRINO_USER": "gx user",
                "TRINO_CATALOG": "lakehouse",
                "TRINO_SCHEMA": "silver",
            },
            clear=True,
        ):
            self.assertEqual(
                smoke._trino_url(),
                "trino://gx%20user@query.example:8443/lakehouse/silver",
            )

    def test_required_environment_rejects_blank_value(self):
        with patch.dict(os.environ, {"NESSIE_BRANCH": "  "}, clear=True):
            with self.assertRaisesRegex(ValueError, "NESSIE_BRANCH"):
                smoke._required_env("NESSIE_BRANCH")

    def test_nessie_reference_requires_requested_branch(self):
        response = MagicMock()
        response.__enter__.return_value = BytesIO(
            json.dumps(
                {"type": "BRANCH", "name": "candidate_123", "hash": "abc"}
            ).encode()
        )
        response.__exit__.return_value = False
        with patch("gx_runtime.smoke.urlopen", return_value=response) as request:
            result = smoke._nessie_reference(
                "http://nessie:19120/api/v1", "candidate_123"
            )

        self.assertEqual(result["hash"], "abc")
        request.assert_called_once_with(
            "http://nessie:19120/api/v1/trees/tree/candidate_123", timeout=15
        )

    def test_nessie_reference_rejects_wrong_reference(self):
        response = MagicMock()
        response.__enter__.return_value = BytesIO(
            json.dumps({"type": "TAG", "name": "candidate_123"}).encode()
        )
        response.__exit__.return_value = False
        with patch("gx_runtime.smoke.urlopen", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "requested branch"):
                smoke._nessie_reference(
                    "http://nessie:19120/api/v1", "candidate_123"
                )


if __name__ == "__main__":
    unittest.main()
