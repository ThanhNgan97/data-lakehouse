"""Run the bounded phase-6 benchmark against the local Trino/Nessie stack."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, event, text

from gx_runtime.benchmark import (
    BenchmarkCase,
    BenchmarkRunner,
    WorkloadMetrics,
    median_sample,
)
from gx_runtime.engines import VersionedSqlSuiteExecutor
from gx_runtime.models import ExecutionEngine, ExecutionMode, SuiteReference, ValidationRequest
from gx_runtime.policy import ValidationPolicy
from gx_runtime.snapshot_gate import NessieSnapshotResolver
from gx_runtime.suite_registry import SuiteRegistry


def trino_url() -> str:
    return (
        f"trino://{os.getenv('TRINO_USER', 'gx-validator')}@"
        f"{os.getenv('TRINO_HOST', 'trino')}:"
        f"{os.getenv('TRINO_PORT', '8080')}/lakehouse/default"
    )


class TrinoStats:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.bytes_scanned = 0
        self.scan_seconds = 0.0
        self.query_count = 0

    def capture(self, _conn, cursor, _statement, _parameters, _context, _many):
        stats = getattr(cursor, "stats", {}) or {}
        self.bytes_scanned += int(
            stats.get("physicalInputBytes") or stats.get("processedBytes") or 0
        )
        self.scan_seconds += float(stats.get("wallTimeMillis") or 0) / 1000
        self.query_count += 1


def request(name, layer, asset, identity, mode, run_id):
    return ValidationRequest(
        data_asset_ref=asset,
        dataset_id="education.teaching_progress",
        data_layer=layer,
        execution_engine=ExecutionEngine.SQL,
        contract_version="1.0.0",
        suites=(SuiteReference(name, "1.0.0"),),
        run_id=run_id,
        execution_mode=mode,
        nessie_branch=identity.nessie_branch,
        snapshot_id=identity.snapshot_id,
        nessie_commit_hash=identity.nessie_commit_hash,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args()
    if args.warmups < 0 or args.runs < 1:
        parser.error("warmups must be >= 0 and runs must be >= 1")

    registry = SuiteRegistry(
        os.getenv("GX_SUITE_REGISTRY", "/opt/gx/config/suite_registry.json")
    )
    engine = create_engine(trino_url(), pool_pre_ping=True)
    stats = TrinoStats()
    event.listen(engine, "after_cursor_execute", stats.capture)
    executor = VersionedSqlSuiteExecutor(registry, engine)
    resolver = NessieSnapshotResolver(
        os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1")
    )
    silver_asset = "lakehouse.silver.teaching_progress"
    gold_asset = "lakehouse.gold.teaching_progress_metrics"
    with engine.connect() as connection:
        silver_rows = connection.execute(
            text(f"SELECT count(*) FROM {silver_asset}")
        ).scalar_one()
        gold_rows = connection.execute(
            text(f"SELECT count(*) FROM {gold_asset}")
        ).scalar_one()
        silver_bytes = connection.execute(
            text(
                'SELECT sum(file_size_in_bytes) FROM '
                'lakehouse.silver."teaching_progress$files"'
            )
        ).scalar_one()
        gold_bytes = connection.execute(
            text(
                'SELECT sum(file_size_in_bytes) FROM '
                'lakehouse.gold."teaching_progress_metrics$files"'
            )
        ).scalar_one()
    dataset_bytes = int(silver_bytes or 0) + int(gold_bytes or 0)
    stats.reset()

    def operation(mode: ExecutionMode, sequence: int) -> WorkloadMetrics:
        candidate_start = time.perf_counter()
        silver_identity = resolver.resolve(silver_asset, "main")
        gold_identity = resolver.resolve(gold_asset, "main")
        candidate_duration = time.perf_counter() - candidate_start
        if mode is ExecutionMode.OFF:
            return WorkloadMetrics(
                candidate_query_duration_seconds=candidate_duration,
                details={"silver_rows": silver_rows, "gold_rows": gold_rows},
            )

        stats.reset()
        silver_start = time.perf_counter()
        silver_request = request(
            "teaching_progress_silver_core", "silver", silver_asset,
            silver_identity, mode, f"phase6-{mode.value}-{sequence}-silver",
        )
        silver_result = executor(silver_request)
        silver_duration = time.perf_counter() - silver_start

        gold_start = time.perf_counter()
        gold_request = request(
            "teaching_progress_gold_core", "gold", gold_asset,
            gold_identity, mode, f"phase6-{mode.value}-{sequence}-gold",
        )
        gold_result = executor(gold_request)
        reconciliation_request = request(
            "teaching_progress_silver_gold_reconciliation", "gold", gold_asset,
            gold_identity, mode, f"phase6-{mode.value}-{sequence}-reconciliation",
        )
        reconciliation_result = executor(reconciliation_request)
        gold_duration = time.perf_counter() - gold_start
        summaries = (
            ValidationPolicy().apply(silver_request, silver_result),
            ValidationPolicy().apply(gold_request, gold_result),
            ValidationPolicy().apply(reconciliation_request, reconciliation_result),
        )
        return WorkloadMetrics(
            gx_silver_duration_seconds=silver_duration,
            gx_gold_duration_seconds=gold_duration,
            candidate_query_duration_seconds=candidate_duration,
            trino_scan_duration_seconds=stats.scan_seconds,
            trino_bytes_scanned=stats.bytes_scanned,
            detected_violations=sum(item.failed_expectations for item in summaries),
            critical_rows_published=0,
            branch_merged=False,
            details={
                "silver_rows": silver_rows,
                "gold_rows": gold_rows,
                "trino_query_count": stats.query_count,
                "statuses": [item.status.value for item in summaries],
            },
        )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warmup_count": args.warmups,
        "measured_runs": args.runs,
        "dataset": "education.teaching_progress",
        "read_only": True,
        "cases": [],
    }
    runner = BenchmarkRunner()
    sequence = 0
    for mode in ExecutionMode:
        case = BenchmarkCase(
            name=f"teaching-progress-{mode.value}",
            mode=mode,
            dataset_rows=int(silver_rows),
            dataset_bytes=dataset_bytes,
            injected_violations=0,
        )
        for _ in range(args.warmups):
            sequence += 1
            runner.measure(case, lambda m=mode, s=sequence: operation(m, s))
        samples = []
        for _ in range(args.runs):
            sequence += 1
            samples.append(
                runner.measure(case, lambda m=mode, s=sequence: operation(m, s))
            )
        case_result = {"median": median_sample(samples)}
        if not args.summary_only:
            case_result["samples"] = [sample.as_dict() for sample in samples]
        report["cases"].append(case_result)
    engine.dispose()
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
