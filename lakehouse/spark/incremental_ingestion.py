"""Incremental API ingestion coordination.

This module owns the F3 state machine:

checkpoint read
-> source lower bound
-> API fetch
-> candidate watermark
-> Bronze write/read-back validation
-> checkpoint compare-and-set

Checkpoint persistence, timestamp semantics, HTTP resilience/pagination, and
Bronze mechanics remain delegated to their existing specialized components.
"""

from __future__ import annotations

import hashlib
import json

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

from api_dataset_registry import get_dataset_config
from api_ingestion import (
    IngestionResult,
    fetch_http_api_payload,
    ingest_payload_to_bronze,
)
from checkpoint_store import (
    CheckpointState,
    MinioCheckpointStore,
)
from pagination import PaginationStrategy
from watermark_strategy import resolve_watermark_strategy


STATUS_COMMITTED = "COMMITTED"
STATUS_NO_CHANGE = "NO_CHANGE"


FetchPayload = Callable[..., dict[str, Any]]
IngestPayload = Callable[..., IngestionResult]


def logical_batch_id(
    dataset_id: str,
    checkpoint_before: CheckpointState | None,
) -> str:
    """Return a stable batch id for one safe source checkpoint."""
    if (
        not isinstance(dataset_id, str)
        or not dataset_id.strip()
    ):
        raise ValueError(
            "dataset_id must be a non-empty string"
        )

    if checkpoint_before is None:
        identity = {
            "dataset_id": dataset_id,
            "strategy_type": None,
            "checkpoint_payload": None,
        }
    else:
        if checkpoint_before.dataset_id != dataset_id:
            raise ValueError(
                "checkpoint dataset does not match dataset_id"
            )

        identity = {
            "dataset_id": dataset_id,
            "strategy_type": checkpoint_before.strategy_type,
            "checkpoint_payload": (
                checkpoint_before.checkpoint_payload
            ),
        }

    canonical = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()[:20]

    return f"airflow_api_{digest}"

class CheckpointStrategyMismatchError(RuntimeError):
    """Stored checkpoint strategy disagrees with current dataset config."""


@dataclass(frozen=True)
class IncrementalFetchResult:
    dataset_id: str
    fetched_count: int
    updated_after: str | None
    checkpoint_before: CheckpointState | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class IncrementalIngestionResult:
    status: str
    dataset_id: str
    batch_id: str
    fetched_count: int
    updated_after: str | None
    checkpoint_before: CheckpointState | None
    checkpoint_after: CheckpointState | None
    candidate_checkpoint: dict[str, Any] | None
    bronze_result: IngestionResult | None


def fetch_incremental_payload(
    api_url: str,
    *,
    dataset: str,
    api_key: str | None = None,
    page_limit: int = 500,
    timeout_seconds: int = 30,
    pagination_strategy: PaginationStrategy | None = None,
    checkpoint_store: Any | None = None,
    fetch_payload: FetchPayload = fetch_http_api_payload,
) -> IncrementalFetchResult:
    """Read checkpoint state and fetch one incremental API payload.

    This operation is read-only with respect to checkpoint persistence.
    It performs no Bronze write and no checkpoint compare-and-set.
    """
    config = get_dataset_config(dataset)

    store = (
        checkpoint_store
        if checkpoint_store is not None
        else MinioCheckpointStore()
    )

    checkpoint_before = store.load(
        config.dataset
    )

    strategy = resolve_watermark_strategy(
        config.watermark_strategy
    )

    if (
        checkpoint_before is not None
        and checkpoint_before.strategy_type
        != strategy.strategy_type
    ):
        raise CheckpointStrategyMismatchError(
            "stored checkpoint strategy "
            f"{checkpoint_before.strategy_type!r} "
            "does not match configured strategy "
            f"{strategy.strategy_type!r} "
            f"for dataset {config.dataset!r}"
        )

    previous_payload = (
        checkpoint_before.checkpoint_payload
        if checkpoint_before is not None
        else None
    )

    updated_after = strategy.request_watermark(
        previous_payload
    )

    payload = fetch_payload(
        api_url,
        dataset=config.dataset,
        updated_after=updated_after,
        api_key=api_key,
        page_limit=page_limit,
        timeout_seconds=timeout_seconds,
        pagination_strategy=pagination_strategy,
    )

    return IncrementalFetchResult(
        dataset_id=config.dataset,
        fetched_count=len(payload["data"]),
        updated_after=updated_after,
        checkpoint_before=checkpoint_before,
        payload=payload,
    )


def run_incremental_ingestion(
    spark: SparkSession,
    api_url: str,
    *,
    dataset: str,
    batch_id: str | None = None,
    api_key: str | None = None,
    page_limit: int = 500,
    timeout_seconds: int = 30,
    pagination_strategy: PaginationStrategy | None = None,
    checkpoint_store: Any | None = None,
    fetch_payload: FetchPayload = fetch_http_api_payload,
    ingest_payload: IngestPayload = ingest_payload_to_bronze,
) -> IncrementalIngestionResult:
    """Run one F3 incremental ingestion attempt.

    A checkpoint is advanced only after ``ingest_payload`` returns
    successfully. In the current Bronze implementation, that return occurs
    only after physical write and read-back validation succeed.
    """
    if batch_id is not None and (
        not isinstance(batch_id, str)
        or not batch_id.strip()
    ):
        raise ValueError(
            "batch_id must be a non-empty string"
        )

    config = get_dataset_config(dataset)

    store = (
        checkpoint_store
        if checkpoint_store is not None
        else MinioCheckpointStore()
    )

    fetched = fetch_incremental_payload(
        api_url,
        dataset=config.dataset,
        api_key=api_key,
        page_limit=page_limit,
        timeout_seconds=timeout_seconds,
        pagination_strategy=pagination_strategy,
        checkpoint_store=store,
        fetch_payload=fetch_payload,
    )

    checkpoint_before = fetched.checkpoint_before

    effective_batch_id = (
        batch_id
        if batch_id is not None
        else logical_batch_id(
            config.dataset,
            checkpoint_before,
        )
    )
    updated_after = fetched.updated_after
    payload = fetched.payload
    records = payload["data"]
    fetched_count = fetched.fetched_count

    strategy = resolve_watermark_strategy(
        config.watermark_strategy
    )

    previous_payload = (
        checkpoint_before.checkpoint_payload
        if checkpoint_before is not None
        else None
    )

    if fetched_count == 0:
        return IncrementalIngestionResult(
            status=STATUS_NO_CHANGE,
            dataset_id=config.dataset,
            batch_id=effective_batch_id,
            fetched_count=0,
            updated_after=updated_after,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_before,
            candidate_checkpoint=None,
            bronze_result=None,
        )

    candidate = strategy.candidate(
        records,
        field_name=config.source_updated_at_field,
    )

    if candidate is None:
        raise RuntimeError(
            "non-empty incremental payload produced "
            "no checkpoint candidate"
        )

    strategy.validate_advance(
        previous_payload,
        candidate,
    )

    bronze_result = ingest_payload(
        spark,
        payload,
        dataset=config.dataset,
        ingestion_mode="INCREMENTAL",
        batch_id=effective_batch_id,
        expected_count=fetched_count,
        input_label=f"HTTP GET {api_url} incremental",
    )

    checkpoint_after = store.compare_and_set(
        dataset_id=config.dataset,
        strategy_type=strategy.strategy_type,
        expected_version=(
            checkpoint_before.version
            if checkpoint_before is not None
            else None
        ),
        checkpoint_payload=candidate,
        batch_id=bronze_result.batch_id,
    )

    return IncrementalIngestionResult(
        status=STATUS_COMMITTED,
        dataset_id=config.dataset,
        batch_id=bronze_result.batch_id,
        fetched_count=fetched_count,
        updated_after=updated_after,
        checkpoint_before=checkpoint_before,
        checkpoint_after=checkpoint_after,
        candidate_checkpoint=candidate,
        bronze_result=bronze_result,
    )
