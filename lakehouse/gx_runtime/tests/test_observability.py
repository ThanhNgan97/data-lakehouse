from __future__ import annotations

import unittest

from gx_runtime.api import _caused_by_timeout
from gx_runtime.errors import ValidationExecutionError
from gx_runtime.observability import GXMetrics


class ObservabilityTest(unittest.TestCase):
    def test_required_metrics_are_exposed_without_identity_labels(self):
        metrics = GXMetrics()
        metrics.observe_validation(
            duration_seconds=1.25,
            layer="silver",
            engine="spark",
            status="PASSED",
            rows=42,
            bytes_scanned=1024,
        )
        metrics.record_timeout("spark")
        metrics.record_engine_error("spark")
        metrics.record_retry("spark")
        metrics.record_quality_failure("silver", "spark")

        output = metrics.render()
        for name in (
            "gx_validation_duration_seconds",
            "gx_validation_rows",
            "gx_validation_bytes_scanned",
            "gx_validation_timeout_total",
            "gx_engine_error_total",
            "gx_retry_total",
            "gx_quality_failure_total",
        ):
            self.assertIn(name, output)
        self.assertNotIn("run_id", output)
        self.assertNotIn("snapshot_id", output)
        self.assertNotIn("commit_hash", output)

    def test_histogram_and_counters_accumulate(self):
        metrics = GXMetrics()
        metrics.observe_validation(
            duration_seconds=0.2,
            layer="gold",
            engine="sql",
            status="FAILED",
        )
        metrics.observe_validation(
            duration_seconds=2.0,
            layer="gold",
            engine="sql",
            status="FAILED",
        )
        metrics.record_quality_failure("gold", "sql")
        metrics.record_quality_failure("gold", "sql")

        output = metrics.render()
        self.assertIn(
            'gx_validation_duration_seconds_count{engine="sql",layer="gold",status="FAILED"} 2',
            output,
        )
        self.assertIn(
            'gx_quality_failure_total{engine="sql",layer="gold"} 2', output
        )

    def test_wrapped_timeout_is_detected(self):
        try:
            try:
                raise TimeoutError("query timeout")
            except TimeoutError as exc:
                raise ValidationExecutionError("engine failed") from exc
        except ValidationExecutionError as error:
            self.assertTrue(_caused_by_timeout(error))


if __name__ == "__main__":
    unittest.main()
