"""Unit tests for dependency checks before a heavy generic Spark run."""

import os
import unittest
from unittest.mock import patch

import service_preflight


class ServicePreflightTest(unittest.TestCase):
    @patch.dict(os.environ, {
        "NESSIE_API_URL": "http://nessie:19120/api/v1",
        "S3_ENDPOINT": "http://s3g:9878",
    })
    @patch("service_preflight._nessie_ready")
    @patch("service_preflight._tcp_ready")
    def test_checks_all_services(self, tcp_ready, nessie_ready):
        service_preflight.check_services(attempts=1, delay=0)
        self.assertEqual(tcp_ready.call_count, 2)
        tcp_ready.assert_any_call("postgres", 5432)
        tcp_ready.assert_any_call("s3g", 9878)
        nessie_ready.assert_called_once_with("http://nessie:19120/api/v1")

    @patch.dict(os.environ, {"S3_ENDPOINT": "http://s3g:9878"})
    @patch("service_preflight.time.sleep")
    @patch("service_preflight._tcp_ready", side_effect=OSError("DNS unavailable"))
    def test_fails_after_bounded_retries(self, tcp_ready, sleep):
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            service_preflight.check_services(attempts=3, delay=0)
        self.assertEqual(tcp_ready.call_count, 3)
        self.assertEqual(sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
