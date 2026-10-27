from __future__ import annotations

import io
import unittest
from unittest.mock import MagicMock, patch

from gx_runtime.api import TableValidationApplication
from gx_runtime.healthcheck import check


class KubernetesReadinessTest(unittest.TestCase):
    def test_close_marks_service_unready_and_disposes_pool(self):
        application = TableValidationApplication.__new__(TableValidationApplication)
        application.accepting_requests = True
        application.sqlalchemy_engine = MagicMock()

        application.close()

        self.assertFalse(application.accepting_requests)
        application.sqlalchemy_engine.dispose.assert_called_once_with()

    @patch("gx_runtime.healthcheck.urlopen")
    def test_readiness_command_succeeds_only_on_http_200(self, open_url):
        response = MagicMock(status=200)
        response.__enter__.return_value = response
        open_url.return_value = response

        self.assertEqual(check("ready"), 0)
        open_url.assert_called_once_with(
            "http://127.0.0.1:8090/health/ready", timeout=3.0
        )

    @patch("gx_runtime.healthcheck.urlopen", side_effect=OSError("offline"))
    def test_probe_failure_is_nonzero_and_retry_safe(self, _open_url):
        with patch("sys.stderr", new=io.StringIO()):
            self.assertEqual(check("ready"), 1)
            self.assertEqual(check("ready"), 1)

    def test_unknown_probe_is_configuration_error(self):
        with patch("sys.stderr", new=io.StringIO()):
            self.assertEqual(check("startup"), 2)


if __name__ == "__main__":
    unittest.main()
