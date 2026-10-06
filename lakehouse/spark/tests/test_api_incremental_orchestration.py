from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import Mock, patch

from api_dataset_registry import get_dataset_config
from api_ingestion import IngestionResult
from checkpoint_store import CheckpointState
from incremental_ingestion import (
    IncrementalFetchResult,
    IncrementalIngestionResult,
    STATUS_COMMITTED,
    STATUS_NO_CHANGE,
)

import api_dataset_orchestration as orchestration


DATASET = "education.learning_outcomes"
RUN_ID = "manual__f3_orchestration"
BATCH_ID = orchestration.deterministic_batch_id(
    DATASET,
    RUN_ID,
)


def _config():
    return get_dataset_config(DATASET)


def _spark():
    spark = Mock()
    spark.sparkContext = Mock()
    return spark


def test_preflight_allows_zero_incremental_records():
    config = _config()

    fetch_result = IncrementalFetchResult(
        dataset_id=DATASET,
        fetched_count=0,
        updated_after="2026-10-06T10:00:00Z",
        checkpoint_before=CheckpointState(
            dataset_id=DATASET,
            strategy_type="timestamp",
            checkpoint_payload={
                "value": "2026-10-06T10:00:00Z"
            },
            version=7,
            last_batch_id="batch-7",
        ),
        payload={
            "source_system": "ctu_ioc",
            "data": [],
        },
    )

    output = StringIO()

    with patch.object(
        orchestration,
        "fetch_incremental_payload",
        return_value=fetch_result,
    ) as fetch:
        with redirect_stdout(output):
            result = orchestration.command_api_preflight(
                config,
                RUN_ID,
            )

    assert result is None

    fetch.assert_called_once()

    text = output.getvalue()

    assert "SOURCE_RECORD_COUNT=0" in text
    assert "CHECKPOINT_BEFORE_VERSION=7" in text
    assert (
        "UPDATED_AFTER=2026-10-06T10:00:00Z"
        in text
    )
    assert "INCREMENTAL_PREFLIGHT=True" in text
    assert "API_PREFLIGHT_PASS=True" in text


def test_ingest_committed_prints_gate_evidence():
    config = _config()
    spark = _spark()

    before = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-06T10:00:00Z"
        },
        version=7,
        last_batch_id="batch-7",
    )

    after = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-06T11:00:00Z"
        },
        version=8,
        last_batch_id=BATCH_ID,
    )

    bronze = IngestionResult(
        object_key="bronze/test/data.parquet",
        input_count=3,
        dataframe_count=3,
        readback_count=3,
        batch_id=BATCH_ID,
        sample_record_id="r1",
        sample_checksum="checksum",
    )

    incremental = IncrementalIngestionResult(
        status=STATUS_COMMITTED,
        dataset_id=DATASET,
        batch_id=BATCH_ID,
        fetched_count=3,
        updated_after="2026-10-06T10:00:00Z",
        checkpoint_before=before,
        checkpoint_after=after,
        candidate_checkpoint={
            "value": "2026-10-06T11:00:00Z"
        },
        bronze_result=bronze,
    )

    output = StringIO()

    with patch.object(
        orchestration,
        "get_spark_session",
        return_value=spark,
    ):
        with patch.object(
            orchestration,
            "run_incremental_ingestion",
            return_value=incremental,
        ) as run:
            with redirect_stdout(output):
                result = (
                    orchestration.command_ingest_bronze(
                        config,
                        RUN_ID,
                    )
                )

    assert result == incremental

    run.assert_called_once()

    spark.stop.assert_called_once()

    text = output.getvalue()

    assert "INGESTION_STATUS=COMMITTED" in text
    assert "SOURCE_RECORD_COUNT=3" in text
    assert "CHECKPOINT_BEFORE_VERSION=7" in text
    assert "CHECKPOINT_AFTER_VERSION=8" in text
    assert "BRONZE_READBACK_COUNT=3" in text
    assert "BRONZE_WRITTEN=True" in text
    assert "CHECKPOINT_ADVANCED=True" in text
    assert "INGEST_BRONZE_PASS=True" in text


def test_ingest_no_change_has_no_bronze_evidence():
    config = _config()
    spark = _spark()

    before = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-06T10:00:00Z"
        },
        version=7,
        last_batch_id="batch-7",
    )

    incremental = IncrementalIngestionResult(
        status=STATUS_NO_CHANGE,
        dataset_id=DATASET,
        batch_id=BATCH_ID,
        fetched_count=0,
        updated_after="2026-10-06T10:00:00Z",
        checkpoint_before=before,
        checkpoint_after=before,
        candidate_checkpoint=None,
        bronze_result=None,
    )

    output = StringIO()

    with patch.object(
        orchestration,
        "get_spark_session",
        return_value=spark,
    ):
        with patch.object(
            orchestration,
            "run_incremental_ingestion",
            return_value=incremental,
        ):
            with redirect_stdout(output):
                result = (
                    orchestration.command_ingest_bronze(
                        config,
                        RUN_ID,
                    )
                )

    assert result == incremental

    spark.stop.assert_called_once()

    text = output.getvalue()

    assert "INGESTION_STATUS=NO_CHANGE" in text
    assert "SOURCE_RECORD_COUNT=0" in text
    assert "BRONZE_WRITTEN=False" in text
    assert "BRONZE_READBACK_COUNT=0" in text
    assert "CHECKPOINT_ADVANCED=False" in text
    assert "INGEST_BRONZE_NO_CHANGE=True" in text
    assert "INGEST_BRONZE_PASS=True" not in text


def test_ingest_failure_still_stops_spark():
    config = _config()
    spark = _spark()

    with patch.object(
        orchestration,
        "get_spark_session",
        return_value=spark,
    ):
        with patch.object(
            orchestration,
            "run_incremental_ingestion",
            side_effect=RuntimeError(
                "incremental failure"
            ),
        ):
            try:
                orchestration.command_ingest_bronze(
                    config,
                    RUN_ID,
                )
            except RuntimeError as exc:
                assert str(exc) == "incremental failure"
            else:
                raise AssertionError(
                    "expected incremental failure"
                )

    spark.stop.assert_called_once()
