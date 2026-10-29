from __future__ import annotations

import unittest

from gx_runtime.benchmark import (
    BenchmarkCase,
    BenchmarkRunner,
    WorkloadMetrics,
    median_sample,
)
from gx_runtime.models import ExecutionMode


class BenchmarkRunnerTest(unittest.TestCase):
    def test_off_mode_reports_na_and_zero_gx_metrics(self):
        case = BenchmarkCase("off", ExecutionMode.OFF, 30, 100, 4)
        sample = BenchmarkRunner().measure(
            case,
            lambda: WorkloadMetrics(
                gx_silver_duration_seconds=9,
                gx_gold_duration_seconds=8,
                trino_scan_duration_seconds=7,
                trino_bytes_scanned=6,
                detected_violations=4,
            ),
        )

        self.assertIsNone(sample.detection_rate)
        self.assertIsNone(sample.false_negatives)
        self.assertEqual(sample.gx_silver_duration_seconds, 0)
        self.assertEqual(sample.gx_gold_duration_seconds, 0)
        self.assertEqual(sample.trino_bytes_scanned, 0)
        self.assertEqual(sample.as_dict()["detection_rate"], "N/A")

    def test_report_and_enforce_calculate_detection(self):
        for mode in (ExecutionMode.REPORT, ExecutionMode.ENFORCE):
            with self.subTest(mode=mode):
                sample = BenchmarkRunner().measure(
                    BenchmarkCase(mode.value, mode, 1000, 4096, 10),
                    lambda: WorkloadMetrics(detected_violations=9),
                )
                self.assertEqual(sample.detection_rate, 0.9)
                self.assertEqual(sample.false_negatives, 1)

    def test_median_requires_same_case_and_uses_measured_samples(self):
        runner = BenchmarkRunner()
        case = BenchmarkCase("enforce", ExecutionMode.ENFORCE, 30, None, 0)
        samples = [
            runner.measure(case, lambda: WorkloadMetrics()),
            runner.measure(case, lambda: WorkloadMetrics()),
        ]
        result = median_sample(samples)
        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["case_name"], "enforce")

    def test_invalid_concurrency_is_rejected(self):
        with self.assertRaises(ValueError):
            BenchmarkCase("bad", ExecutionMode.ENFORCE, 1, 1, 0, 0)


if __name__ == "__main__":
    unittest.main()
