from unittest.mock import Mock

import pytest

from api_ingestion import IngestionResult
from checkpoint_store import (
    CheckpointConflictError,
    CheckpointState,
)
from incremental_ingestion import (
    CheckpointStrategyMismatchError,
    STATUS_COMMITTED,
    STATUS_NO_CHANGE,
    run_incremental_ingestion,
)


DATASET = "education.learning_outcomes"
API_URL = "http://mock/api/v1/education/learning-outcomes"
BATCH_ID = "batch-f3-test"


def _bronze_result(
    *,
    batch_id=BATCH_ID,
    count=1,
):
    return IngestionResult(
        object_key=(
            "bronze/api/ctu_ioc/education/"
            "learning_outcomes/"
            f"batch_id={batch_id}/data.parquet"
        ),
        input_count=count,
        dataframe_count=count,
        readback_count=count,
        batch_id=batch_id,
        sample_record_id="r1",
        sample_checksum="checksum",
    )


def _record(timestamp):
    return {
        "record_id": "r1",
        "updated_at": timestamp,
    }


def test_first_run_commits_only_after_bronze_success():
    events = []

    store = Mock()

    def load(dataset_id):
        events.append("load")
        assert dataset_id == DATASET
        return None

    def compare_and_set(**kwargs):
        events.append("cas")

        assert kwargs["expected_version"] is None
        assert kwargs["strategy_type"] == "timestamp"
        assert kwargs["checkpoint_payload"] == {
            "value": "2026-10-06T10:00:00Z"
        }
        assert kwargs["batch_id"] == BATCH_ID

        return CheckpointState(
            dataset_id=DATASET,
            strategy_type="timestamp",
            checkpoint_payload=kwargs[
                "checkpoint_payload"
            ],
            version=1,
            last_batch_id=BATCH_ID,
        )

    store.load.side_effect = load
    store.compare_and_set.side_effect = compare_and_set

    fetch = Mock()

    def fetch_side_effect(*args, **kwargs):
        events.append("fetch")

        assert args == (API_URL,)
        assert kwargs["dataset"] == DATASET
        assert kwargs["updated_after"] is None

        return {
            "data": [
                _record(
                    "2026-10-06T10:00:00Z"
                )
            ]
        }

    fetch.side_effect = fetch_side_effect

    bronze = Mock()

    def bronze_side_effect(*args, **kwargs):
        events.append("bronze")

        assert kwargs["dataset"] == DATASET
        assert kwargs["ingestion_mode"] == "INCREMENTAL"
        assert kwargs["batch_id"] == BATCH_ID
        assert kwargs["expected_count"] == 1

        return _bronze_result()

    bronze.side_effect = bronze_side_effect

    result = run_incremental_ingestion(
        Mock(),
        API_URL,
        dataset=DATASET,
        batch_id=BATCH_ID,
        checkpoint_store=store,
        fetch_payload=fetch,
        ingest_payload=bronze,
    )

    assert events == [
        "load",
        "fetch",
        "bronze",
        "cas",
    ]

    assert result.status == STATUS_COMMITTED
    assert result.fetched_count == 1
    assert result.updated_after is None
    assert result.checkpoint_before is None
    assert result.checkpoint_after.version == 1


def test_existing_checkpoint_becomes_updated_after():
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

    store = Mock()
    store.load.return_value = before
    store.compare_and_set.return_value = after

    fetch = Mock(
        return_value={
            "data": [
                _record(
                    "2026-10-06T11:00:00Z"
                )
            ]
        }
    )

    bronze = Mock(
        return_value=_bronze_result()
    )

    result = run_incremental_ingestion(
        Mock(),
        API_URL,
        dataset=DATASET,
        batch_id=BATCH_ID,
        api_key="test-key",
        page_limit=123,
        timeout_seconds=45,
        checkpoint_store=store,
        fetch_payload=fetch,
        ingest_payload=bronze,
    )

    fetch.assert_called_once_with(
        API_URL,
        dataset=DATASET,
        updated_after="2026-10-06T10:00:00Z",
        api_key="test-key",
        page_limit=123,
        timeout_seconds=45,
        pagination_strategy=None,
    )

    store.compare_and_set.assert_called_once_with(
        dataset_id=DATASET,
        strategy_type="timestamp",
        expected_version=7,
        checkpoint_payload={
            "value": "2026-10-06T11:00:00Z"
        },
        batch_id=BATCH_ID,
    )

    assert result.status == STATUS_COMMITTED
    assert result.checkpoint_before == before
    assert result.checkpoint_after == after


def test_no_change_skips_bronze_and_checkpoint_write():
    before = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-06T10:00:00Z"
        },
        version=7,
        last_batch_id="batch-7",
    )

    store = Mock()
    store.load.return_value = before

    fetch = Mock(
        return_value={
            "data": [],
        }
    )

    bronze = Mock()

    result = run_incremental_ingestion(
        Mock(),
        API_URL,
        dataset=DATASET,
        batch_id=BATCH_ID,
        checkpoint_store=store,
        fetch_payload=fetch,
        ingest_payload=bronze,
    )

    assert result.status == STATUS_NO_CHANGE
    assert result.fetched_count == 0
    assert result.checkpoint_before == before
    assert result.checkpoint_after == before
    assert result.candidate_checkpoint is None
    assert result.bronze_result is None

    bronze.assert_not_called()
    store.compare_and_set.assert_not_called()


def test_fetch_failure_never_writes_bronze_or_checkpoint():
    store = Mock()
    store.load.return_value = None

    fetch = Mock(
        side_effect=RuntimeError("fetch failed")
    )

    bronze = Mock()

    with pytest.raises(
        RuntimeError,
        match="fetch failed",
    ):
        run_incremental_ingestion(
            Mock(),
            API_URL,
            dataset=DATASET,
            batch_id=BATCH_ID,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )

    bronze.assert_not_called()
    store.compare_and_set.assert_not_called()


def test_bad_watermark_record_never_writes_bronze_or_checkpoint():
    store = Mock()
    store.load.return_value = None

    fetch = Mock(
        return_value={
            "data": [
                {
                    "record_id": "r1",
                }
            ],
        }
    )

    bronze = Mock()

    with pytest.raises(
        ValueError,
        match="missing watermark field",
    ):
        run_incremental_ingestion(
            Mock(),
            API_URL,
            dataset=DATASET,
            batch_id=BATCH_ID,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )

    bronze.assert_not_called()
    store.compare_and_set.assert_not_called()


def test_non_advancing_candidate_never_writes_bronze_or_checkpoint():
    before = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-06T10:00:00Z"
        },
        version=7,
        last_batch_id="batch-7",
    )

    store = Mock()
    store.load.return_value = before

    fetch = Mock(
        return_value={
            "data": [
                _record(
                    "2026-10-06T10:00:00Z"
                )
            ],
        }
    )

    bronze = Mock()

    with pytest.raises(
        ValueError,
        match="greater than",
    ):
        run_incremental_ingestion(
            Mock(),
            API_URL,
            dataset=DATASET,
            batch_id=BATCH_ID,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )

    bronze.assert_not_called()
    store.compare_and_set.assert_not_called()


def test_bronze_failure_does_not_advance_checkpoint():
    store = Mock()
    store.load.return_value = None

    fetch = Mock(
        return_value={
            "data": [
                _record(
                    "2026-10-06T10:00:00Z"
                )
            ],
        }
    )

    bronze = Mock(
        side_effect=RuntimeError(
            "bronze write failed"
        )
    )

    with pytest.raises(
        RuntimeError,
        match="bronze write failed",
    ):
        run_incremental_ingestion(
            Mock(),
            API_URL,
            dataset=DATASET,
            batch_id=BATCH_ID,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )

    bronze.assert_called_once()
    store.compare_and_set.assert_not_called()


def test_cas_conflict_happens_after_bronze_success():
    events = []

    store = Mock()

    def load(_dataset_id):
        events.append("load")
        return None

    def cas(**_kwargs):
        events.append("cas")
        raise CheckpointConflictError(
            "checkpoint compare-and-set conflict"
        )

    store.load.side_effect = load
    store.compare_and_set.side_effect = cas

    fetch = Mock()

    def fetch_side_effect(*_args, **_kwargs):
        events.append("fetch")

        return {
            "data": [
                _record(
                    "2026-10-06T10:00:00Z"
                )
            ],
        }

    fetch.side_effect = fetch_side_effect

    bronze = Mock()

    def bronze_side_effect(*_args, **_kwargs):
        events.append("bronze")
        return _bronze_result()

    bronze.side_effect = bronze_side_effect

    with pytest.raises(
        CheckpointConflictError,
        match="compare-and-set conflict",
    ):
        run_incremental_ingestion(
            Mock(),
            API_URL,
            dataset=DATASET,
            batch_id=BATCH_ID,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )

    assert events == [
        "load",
        "fetch",
        "bronze",
        "cas",
    ]


def test_strategy_mismatch_fails_before_fetch():
    store = Mock()

    store.load.return_value = CheckpointState(
        dataset_id=DATASET,
        strategy_type="source_cursor",
        checkpoint_payload={
            "value": "opaque"
        },
        version=3,
        last_batch_id="batch-3",
    )

    fetch = Mock()
    bronze = Mock()

    with pytest.raises(
        CheckpointStrategyMismatchError,
        match="does not match configured strategy",
    ):
        run_incremental_ingestion(
            Mock(),
            API_URL,
            dataset=DATASET,
            batch_id=BATCH_ID,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )

    fetch.assert_not_called()
    bronze.assert_not_called()
    store.compare_and_set.assert_not_called()


def test_empty_batch_id_fails_before_checkpoint_load():
    store = Mock()

    with pytest.raises(
        ValueError,
        match="batch_id",
    ):
        run_incremental_ingestion(
            Mock(),
            API_URL,
            dataset=DATASET,
            batch_id="",
            checkpoint_store=store,
        )

    store.load.assert_not_called()
