"""Airflow-facing Bronze technical evidence collectors and GX gate client."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from gx_airflow_adapter import execute_bronze_quality_gate
from gx_route_resolver import resolve_route


def inspect_file(path: str) -> dict[str, Any]:
    source = Path(path)
    supported = source.suffix.lower() in {".csv", ".tsv", ".json", ".parquet", ".xlsx", ".xls"}
    metrics: dict[str, Any] = {
        "readable": False,
        "row_count": 0,
        "corrupt_record_ratio": 0.0,
        "metadata_complete": bool(source.name and source.suffix),
        "checksum_present": False,
        "duplicate_ingestion_count": 0,
        "format_supported": supported,
        "header_valid": False,
    }
    if not source.is_file() or not supported:
        return metrics
    try:
        digest = hashlib.sha256()
        with source.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        metrics["checksum_present"] = bool(digest.hexdigest())
        if source.suffix.lower() in {".csv", ".tsv"}:
            with source.open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t" if source.suffix.lower() == ".tsv" else ",")
                metrics["header_valid"] = bool(reader.fieldnames) and all(
                    str(name or "").strip() for name in reader.fieldnames or []
                )
                metrics["row_count"] = sum(1 for _ in reader)
        elif source.suffix.lower() == ".json":
            value = json.loads(source.read_text(encoding="utf-8-sig"))
            rows = value if isinstance(value, list) else [value]
            metrics["row_count"] = len(rows)
            metrics["header_valid"] = bool(rows) and all(isinstance(row, dict) for row in rows)
        else:
            import pandas as pd

            frame = pd.read_parquet(source) if source.suffix.lower() == ".parquet" else pd.read_excel(source)
            metrics["row_count"] = len(frame.index)
            metrics["header_valid"] = len(frame.columns) > 0
        metrics["readable"] = True
    except Exception:
        metrics["corrupt_record_ratio"] = 1.0
    return metrics


def validate_file_bronze(
    *, dataset_id: str, run_id: str, path: str, artifact_ref: str | None = None
) -> dict[str, Any]:
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="bronze",
        source_kind="file",
        execution_engine="sql",
    )
    return execute_bronze_quality_gate(
        {
            "dataset_id": dataset_id,
            "contract_version": route.contract_version,
            "source_kind": "file",
            "run_id": run_id,
            "artifact_ref": artifact_ref or str(Path(path).resolve()),
            "metrics": inspect_file(path),
        }
    )


def validate_bronze_metrics(
    *, dataset_id: str, source_kind: str, run_id: str,
    artifact_ref: str, metrics: dict[str, Any]
) -> dict[str, Any]:
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="bronze",
        source_kind=source_kind,
        execution_engine="sql",
    )
    return execute_bronze_quality_gate(
        {
            "dataset_id": dataset_id,
            "contract_version": route.contract_version,
            "source_kind": source_kind,
            "run_id": run_id,
            "artifact_ref": artifact_ref,
            "metrics": metrics,
        }
    )
