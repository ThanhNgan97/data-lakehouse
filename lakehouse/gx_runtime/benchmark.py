"""Repeatable benchmark primitives for GX execution modes and engines."""

from __future__ import annotations

import os
import statistics
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from .models import ExecutionMode


@dataclass(frozen=True)
class WorkloadMetrics:
    gx_silver_duration_seconds: float = 0.0
    gx_gold_duration_seconds: float = 0.0
    candidate_query_duration_seconds: float = 0.0
    trino_scan_duration_seconds: float = 0.0
    trino_bytes_scanned: int = 0
    spark_executor_seconds: float = 0.0
    detected_violations: int = 0
    critical_rows_published: int = 0
    branch_merged: bool = False
    timeout_count: int = 0
    retry_count: int = 0
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    mode: ExecutionMode
    dataset_rows: int
    dataset_bytes: int | None
    injected_violations: int
    concurrent_validations: int = 1

    def __post_init__(self) -> None:
        numeric = {
            "dataset_rows": self.dataset_rows,
            "injected_violations": self.injected_violations,
            "concurrent_validations": self.concurrent_validations,
        }
        if any(value < 0 for value in numeric.values()):
            raise ValueError("Benchmark counts cannot be negative")
        if self.concurrent_validations < 1:
            raise ValueError("concurrent_validations must be at least one")


@dataclass(frozen=True)
class BenchmarkSample:
    case_name: str
    mode: ExecutionMode
    dataset_rows: int
    dataset_bytes: int | None
    injected_violations: int
    detected_violations: int
    detection_rate: float | None
    false_negatives: int | None
    critical_rows_published: int
    gx_silver_duration_seconds: float
    gx_gold_duration_seconds: float
    candidate_query_duration_seconds: float
    trino_scan_duration_seconds: float
    trino_bytes_scanned: int
    cpu_seconds: float
    peak_rss_bytes: int | None
    spark_executor_seconds: float
    total_duration_seconds: float
    concurrent_validations: int
    timeout_count: int
    retry_count: int
    branch_merged: bool
    details: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["mode"] = self.mode.value
        payload["detection_rate"] = (
            "N/A" if self.detection_rate is None else self.detection_rate
        )
        payload["false_negatives"] = (
            "N/A" if self.false_negatives is None else self.false_negatives
        )
        return payload


def _peak_rss_bytes() -> int | None:
    try:
        import resource

        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KiB; macOS reports bytes.
        return int(value if os.uname().sysname == "Darwin" else value * 1024)
    except (ImportError, AttributeError, OSError):
        return None


class BenchmarkRunner:
    """Measure one operation without embedding validation or policy logic."""

    def measure(
        self,
        case: BenchmarkCase,
        operation: Callable[[], WorkloadMetrics],
    ) -> BenchmarkSample:
        cpu_start = time.process_time()
        wall_start = time.perf_counter()
        metrics = operation()
        total_duration = time.perf_counter() - wall_start
        cpu_duration = time.process_time() - cpu_start

        if case.mode is ExecutionMode.OFF:
            detection_rate = None
            false_negatives = None
            metrics = WorkloadMetrics(
                candidate_query_duration_seconds=(
                    metrics.candidate_query_duration_seconds
                ),
                spark_executor_seconds=metrics.spark_executor_seconds,
                critical_rows_published=metrics.critical_rows_published,
                branch_merged=metrics.branch_merged,
                timeout_count=metrics.timeout_count,
                retry_count=metrics.retry_count,
                details=metrics.details,
            )
        else:
            detection_rate = (
                metrics.detected_violations / case.injected_violations
                if case.injected_violations
                else None
            )
            false_negatives = max(
                case.injected_violations - metrics.detected_violations, 0
            )

        return BenchmarkSample(
            case_name=case.name,
            mode=case.mode,
            dataset_rows=case.dataset_rows,
            dataset_bytes=case.dataset_bytes,
            injected_violations=case.injected_violations,
            detected_violations=metrics.detected_violations,
            detection_rate=detection_rate,
            false_negatives=false_negatives,
            critical_rows_published=metrics.critical_rows_published,
            gx_silver_duration_seconds=metrics.gx_silver_duration_seconds,
            gx_gold_duration_seconds=metrics.gx_gold_duration_seconds,
            candidate_query_duration_seconds=metrics.candidate_query_duration_seconds,
            trino_scan_duration_seconds=metrics.trino_scan_duration_seconds,
            trino_bytes_scanned=metrics.trino_bytes_scanned,
            cpu_seconds=cpu_duration,
            peak_rss_bytes=_peak_rss_bytes(),
            spark_executor_seconds=metrics.spark_executor_seconds,
            total_duration_seconds=total_duration,
            concurrent_validations=case.concurrent_validations,
            timeout_count=metrics.timeout_count,
            retry_count=metrics.retry_count,
            branch_merged=metrics.branch_merged,
            details=metrics.details,
        )


def median_sample(samples: list[BenchmarkSample]) -> dict[str, Any]:
    if not samples:
        raise ValueError("At least one measured sample is required")
    identity = (
        samples[0].case_name,
        samples[0].mode,
        samples[0].dataset_rows,
        samples[0].concurrent_validations,
    )
    if any(
        (sample.case_name, sample.mode, sample.dataset_rows, sample.concurrent_validations)
        != identity
        for sample in samples
    ):
        raise ValueError("Median samples must belong to the same benchmark case")
    payload = samples[0].as_dict()
    numeric_metrics = (
        "gx_silver_duration_seconds",
        "gx_gold_duration_seconds",
        "candidate_query_duration_seconds",
        "trino_scan_duration_seconds",
        "trino_bytes_scanned",
        "cpu_seconds",
        "peak_rss_bytes",
        "spark_executor_seconds",
        "total_duration_seconds",
    )
    for name in numeric_metrics:
        values = [getattr(sample, name) for sample in samples]
        available = [value for value in values if value is not None]
        payload[name] = statistics.median(available) if available else None
    payload["sample_count"] = len(samples)
    return payload
