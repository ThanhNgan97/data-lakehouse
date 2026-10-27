"""Small dependency-free Prometheus registry for the GX HTTP boundary."""

from __future__ import annotations

from collections import defaultdict
from threading import Lock


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(**values: str) -> str:
    return "{" + ",".join(
        f'{name}="{_escape(str(value))}"' for name, value in sorted(values.items())
    ) + "}"


class GXMetrics:
    """Thread-safe metrics with deliberately low-cardinality label sets."""

    _BUCKETS = (0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 300.0)

    def __init__(self) -> None:
        self._lock = Lock()
        self._durations: dict[tuple[str, str, str], list[float]] = defaultdict(list)
        self._rows: dict[tuple[str, str], float] = {}
        self._bytes: dict[tuple[str, str], float] = {}
        self._timeouts: dict[str, int] = defaultdict(int)
        self._engine_errors: dict[str, int] = defaultdict(int)
        self._retries: dict[str, int] = defaultdict(int)
        self._quality_failures: dict[tuple[str, str], int] = defaultdict(int)

    def observe_validation(
        self,
        *,
        duration_seconds: float,
        layer: str,
        engine: str,
        status: str,
        rows: float | None = None,
        bytes_scanned: float | None = None,
    ) -> None:
        with self._lock:
            self._durations[(layer, engine, status)].append(duration_seconds)
            if rows is not None:
                self._rows[(layer, engine)] = rows
            if bytes_scanned is not None:
                self._bytes[(layer, engine)] = bytes_scanned

    def record_timeout(self, engine: str) -> None:
        with self._lock:
            self._timeouts[engine] += 1

    def record_engine_error(self, engine: str) -> None:
        with self._lock:
            self._engine_errors[engine] += 1

    def record_retry(self, engine: str) -> None:
        with self._lock:
            self._retries[engine] += 1

    def record_quality_failure(self, layer: str, engine: str) -> None:
        with self._lock:
            self._quality_failures[(layer, engine)] += 1

    def render(self) -> str:
        with self._lock:
            durations = {key: tuple(values) for key, values in self._durations.items()}
            rows = dict(self._rows)
            scanned = dict(self._bytes)
            timeouts = dict(self._timeouts)
            errors = dict(self._engine_errors)
            retries = dict(self._retries)
            failures = dict(self._quality_failures)

        lines = [
            "# HELP gx_validation_duration_seconds GX validation wall-clock duration.",
            "# TYPE gx_validation_duration_seconds histogram",
        ]
        for (layer, engine, status), values in sorted(durations.items()):
            base = {"layer": layer, "engine": engine, "status": status}
            for bucket in self._BUCKETS:
                lines.append(
                    "gx_validation_duration_seconds_bucket"
                    + _labels(**base, le=str(bucket))
                    + f" {sum(value <= bucket for value in values)}"
                )
            lines.append(
                "gx_validation_duration_seconds_bucket"
                + _labels(**base, le="+Inf")
                + f" {len(values)}"
            )
            lines.append(
                "gx_validation_duration_seconds_sum"
                + _labels(**base)
                + f" {sum(values):.9g}"
            )
            lines.append(
                "gx_validation_duration_seconds_count"
                + _labels(**base)
                + f" {len(values)}"
            )
        lines.extend(self._render_gauge("gx_validation_rows", rows))
        lines.extend(self._render_gauge("gx_validation_bytes_scanned", scanned))
        lines.extend(self._render_counter("gx_validation_timeout_total", timeouts))
        lines.extend(self._render_counter("gx_engine_error_total", errors))
        lines.extend(self._render_counter("gx_retry_total", retries))
        lines.extend(self._render_counter("gx_quality_failure_total", failures))
        return "\n".join(lines) + "\n"

    @staticmethod
    def _render_gauge(name: str, values: dict[tuple[str, str], float]) -> list[str]:
        lines = [f"# TYPE {name} gauge"]
        for (layer, engine), value in sorted(values.items()):
            lines.append(name + _labels(layer=layer, engine=engine) + f" {value:.9g}")
        return lines

    @staticmethod
    def _render_counter(name: str, values: dict) -> list[str]:
        lines = [f"# TYPE {name} counter"]
        for key, value in sorted(values.items(), key=lambda item: str(item[0])):
            labels = (
                {"layer": key[0], "engine": key[1]}
                if isinstance(key, tuple)
                else {"engine": key}
            )
            lines.append(name + _labels(**labels) + f" {value}")
        return lines
