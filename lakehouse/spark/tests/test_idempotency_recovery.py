from checkpoint_store import CheckpointState
from incremental_ingestion import logical_batch_id


DATASET = "education.learning_outcomes"


def _checkpoint(
    value,
    *,
    version=1,
    last_batch_id="old-batch",
    strategy_type="timestamp",
):
    return CheckpointState(
        dataset_id=DATASET,
        strategy_type=strategy_type,
        checkpoint_payload={
            "value": value,
        },
        version=version,
        last_batch_id=last_batch_id,
    )


def test_first_run_batch_is_stable():
    first = logical_batch_id(
        DATASET,
        None,
    )
    second = logical_batch_id(
        DATASET,
        None,
    )

    assert first == second
    assert first.startswith("airflow_api_")


def test_same_safe_checkpoint_gives_same_batch():
    before_a = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=3,
        last_batch_id="batch-a",
    )

    before_b = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=99,
        last_batch_id="batch-b",
    )

    assert (
        logical_batch_id(DATASET, before_a)
        == logical_batch_id(DATASET, before_b)
    )


def test_different_checkpoint_gives_new_batch():
    before = _checkpoint(
        "2026-10-07T01:00:00Z"
    )

    after = _checkpoint(
        "2026-10-07T02:00:00Z"
    )

    assert (
        logical_batch_id(DATASET, before)
        != logical_batch_id(DATASET, after)
    )


def test_strategy_is_part_of_batch_identity():
    timestamp = _checkpoint(
        "2026-10-07T01:00:00Z",
        strategy_type="timestamp",
    )

    other = _checkpoint(
        "2026-10-07T01:00:00Z",
        strategy_type="source_cursor",
    )

    assert (
        logical_batch_id(DATASET, timestamp)
        != logical_batch_id(DATASET, other)
    )


def test_checkpoint_dataset_must_match():
    checkpoint = CheckpointState(
        dataset_id="education.teaching_progress",
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-07T01:00:00Z",
        },
        version=1,
        last_batch_id=None,
    )

    try:
        logical_batch_id(
            DATASET,
            checkpoint,
        )
    except ValueError as exc:
        assert (
            "checkpoint dataset"
            in str(exc)
        )
    else:
        raise AssertionError(
            "dataset mismatch must fail"
        )


from unittest.mock import Mock, patch

from api_dataset_registry import get_dataset_config
from api_ingestion import IngestionResult
from incremental_ingestion import (
    IncrementalIngestionResult,
    STATUS_COMMITTED,
    run_incremental_ingestion,
)

import api_dataset_orchestration as orchestration


def test_coordinator_derives_logical_batch():
    before = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=7,
    )

    expected_batch = logical_batch_id(
        DATASET,
        before,
    )

    after = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-07T02:00:00Z",
        },
        version=8,
        last_batch_id=expected_batch,
    )

    store = Mock()
    store.load.return_value = before
    store.compare_and_set.return_value = after

    config = get_dataset_config(DATASET)

    fetch = Mock(
        return_value={
            "data": [
                {
                    config.source_updated_at_field:
                        "2026-10-07T02:00:00Z",
                },
            ],
        }
    )

    bronze = Mock(
        return_value=IngestionResult(
            object_key=(
                config.bronze_prefix
                + "batch_id="
                + expected_batch
                + "/data.parquet"
            ),
            input_count=1,
            dataframe_count=1,
            readback_count=1,
            batch_id=expected_batch,
            sample_record_id="r1",
            sample_checksum="checksum",
        )
    )

    result = run_incremental_ingestion(
        Mock(),
        "http://mock/api",
        dataset=DATASET,
        checkpoint_store=store,
        fetch_payload=fetch,
        ingest_payload=bronze,
    )

    assert result.status == STATUS_COMMITTED
    assert result.batch_id == expected_batch

    assert (
        bronze.call_args.kwargs["batch_id"]
        == expected_batch
    )

    assert (
        store.compare_and_set.call_args.kwargs[
            "batch_id"
        ]
        == expected_batch
    )


def test_explicit_batch_id_remains_backward_compatible():
    explicit = "legacy-explicit-batch"

    before = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=7,
    )

    store = Mock()
    store.load.return_value = before

    config = get_dataset_config(DATASET)

    fetch = Mock(
        return_value={
            "data": [
                {
                    config.source_updated_at_field:
                        "2026-10-07T02:00:00Z",
                },
            ],
        }
    )

    bronze = Mock(
        side_effect=RuntimeError(
            "stop-after-batch-assertion"
        )
    )

    try:
        run_incremental_ingestion(
            Mock(),
            "http://mock/api",
            dataset=DATASET,
            batch_id=explicit,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )
    except RuntimeError as exc:
        assert str(exc) == "stop-after-batch-assertion"
    else:
        raise AssertionError(
            "expected controlled Bronze stop"
        )

    assert (
        bronze.call_args.kwargs["batch_id"]
        == explicit
    )


def test_orchestration_does_not_supply_run_based_batch():
    config = get_dataset_config(DATASET)

    logical = "airflow_api_logical_test"

    before = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=7,
    )

    after = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-07T02:00:00Z",
        },
        version=8,
        last_batch_id=logical,
    )

    bronze = IngestionResult(
        object_key=(
            config.bronze_prefix
            + "batch_id="
            + logical
            + "/data.parquet"
        ),
        input_count=1,
        dataframe_count=1,
        readback_count=1,
        batch_id=logical,
        sample_record_id="r1",
        sample_checksum="checksum",
    )

    result = IncrementalIngestionResult(
        status=STATUS_COMMITTED,
        dataset_id=DATASET,
        batch_id=logical,
        fetched_count=1,
        updated_after="2026-10-07T01:00:00Z",
        checkpoint_before=before,
        checkpoint_after=after,
        candidate_checkpoint={
            "value": "2026-10-07T02:00:00Z",
        },
        bronze_result=bronze,
    )

    spark = Mock()
    spark.sparkContext = Mock()

    with patch.object(
        orchestration,
        "get_spark_session",
        return_value=spark,
    ):
        with patch.object(
            orchestration,
            "run_incremental_ingestion",
            return_value=result,
        ) as run:
            orchestration.command_ingest_bronze(
                config,
                "manual__different_run_id",
            )

    kwargs = run.call_args.kwargs

    assert "batch_id" not in kwargs


def test_downstream_resolves_committed_batch():
    expected = "airflow_api_committed_batch"

    state = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-07T02:00:00Z",
        },
        version=8,
        last_batch_id=expected,
    )

    store = Mock()
    store.load.return_value = state

    with patch.object(
        orchestration,
        "MinioCheckpointStore",
        return_value=store,
    ):
        actual = (
            orchestration._latest_committed_batch_id(
                DATASET
            )
        )

    assert actual == expected


from checkpoint_store import CheckpointConflictError


def _recovery_bronze_result(
    config,
    *,
    batch_id,
    count,
):
    return IngestionResult(
        object_key=(
            config.bronze_prefix
            + "batch_id="
            + batch_id
            + "/data.parquet"
        ),
        input_count=count,
        dataframe_count=count,
        readback_count=count,
        batch_id=batch_id,
        sample_record_id="r1",
        sample_checksum="checksum",
    )


def test_checkpoint_failure_replay_reuses_same_logical_batch():
    before = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=7,
    )

    expected_batch = logical_batch_id(
        DATASET,
        before,
    )

    after = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-07T02:00:00Z",
        },
        version=8,
        last_batch_id=expected_batch,
    )

    config = get_dataset_config(DATASET)

    store = Mock()
    store.load.return_value = before
    store.compare_and_set.side_effect = [
        CheckpointConflictError(
            "simulated checkpoint failure"
        ),
        after,
    ]

    payload = {
        "data": [
            {
                config.source_updated_at_field:
                    "2026-10-07T02:00:00Z",
            },
        ],
    }

    fetch = Mock(
        side_effect=[
            payload,
            payload,
        ]
    )

    written_batches = []
    written_keys = []

    def bronze_side_effect(
        *_args,
        **kwargs,
    ):
        batch_id = kwargs["batch_id"]
        count = kwargs["expected_count"]

        written_batches.append(batch_id)

        result = _recovery_bronze_result(
            config,
            batch_id=batch_id,
            count=count,
        )

        written_keys.append(
            result.object_key
        )

        return result

    bronze = Mock(
        side_effect=bronze_side_effect
    )

    try:
        run_incremental_ingestion(
            Mock(),
            "http://mock/api",
            dataset=DATASET,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )
    except CheckpointConflictError as exc:
        assert (
            str(exc)
            == "simulated checkpoint failure"
        )
    else:
        raise AssertionError(
            "first run must fail at checkpoint"
        )

    recovered = run_incremental_ingestion(
        Mock(),
        "http://mock/api",
        dataset=DATASET,
        checkpoint_store=store,
        fetch_payload=fetch,
        ingest_payload=bronze,
    )

    assert written_batches == [
        expected_batch,
        expected_batch,
    ]

    assert written_keys[0] == written_keys[1]

    assert bronze.call_count == 2
    assert store.compare_and_set.call_count == 2

    assert recovered.status == STATUS_COMMITTED
    assert recovered.batch_id == expected_batch
    assert recovered.checkpoint_before == before
    assert recovered.checkpoint_after == after

    assert (
        recovered.checkpoint_after.last_batch_id
        == expected_batch
    )


def test_recovery_with_new_source_rows_reuses_batch_and_advances():
    before = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=7,
    )

    expected_batch = logical_batch_id(
        DATASET,
        before,
    )

    config = get_dataset_config(DATASET)

    after = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-07T03:00:00Z",
        },
        version=8,
        last_batch_id=expected_batch,
    )

    first_payload = {
        "data": [
            {
                config.source_updated_at_field:
                    "2026-10-07T01:30:00Z",
            },
            {
                config.source_updated_at_field:
                    "2026-10-07T02:00:00Z",
            },
        ],
    }

    recovery_payload = {
        "data": [
            {
                config.source_updated_at_field:
                    "2026-10-07T01:30:00Z",
            },
            {
                config.source_updated_at_field:
                    "2026-10-07T02:00:00Z",
            },
            {
                config.source_updated_at_field:
                    "2026-10-07T03:00:00Z",
            },
        ],
    }

    store = Mock()
    store.load.return_value = before
    store.compare_and_set.side_effect = [
        CheckpointConflictError(
            "simulated checkpoint failure"
        ),
        after,
    ]

    fetch = Mock(
        side_effect=[
            first_payload,
            recovery_payload,
        ]
    )

    written_batches = []
    written_counts = []
    written_keys = []

    def bronze_side_effect(
        *_args,
        **kwargs,
    ):
        batch_id = kwargs["batch_id"]
        count = kwargs["expected_count"]

        written_batches.append(batch_id)
        written_counts.append(count)

        result = _recovery_bronze_result(
            config,
            batch_id=batch_id,
            count=count,
        )

        written_keys.append(
            result.object_key
        )

        return result

    bronze = Mock(
        side_effect=bronze_side_effect
    )

    try:
        run_incremental_ingestion(
            Mock(),
            "http://mock/api",
            dataset=DATASET,
            checkpoint_store=store,
            fetch_payload=fetch,
            ingest_payload=bronze,
        )
    except CheckpointConflictError:
        pass
    else:
        raise AssertionError(
            "first run must fail at checkpoint"
        )

    recovered = run_incremental_ingestion(
        Mock(),
        "http://mock/api",
        dataset=DATASET,
        checkpoint_store=store,
        fetch_payload=fetch,
        ingest_payload=bronze,
    )

    assert written_batches == [
        expected_batch,
        expected_batch,
    ]

    assert written_keys[0] == written_keys[1]

    assert written_counts == [
        2,
        3,
    ]

    first_cas = (
        store.compare_and_set.call_args_list[
            0
        ].kwargs
    )

    second_cas = (
        store.compare_and_set.call_args_list[
            1
        ].kwargs
    )

    assert first_cas["checkpoint_payload"] == {
        "value": "2026-10-07T02:00:00Z",
    }

    assert second_cas["checkpoint_payload"] == {
        "value": "2026-10-07T03:00:00Z",
    }

    assert (
        first_cas["batch_id"]
        == expected_batch
    )

    assert (
        second_cas["batch_id"]
        == expected_batch
    )

    assert recovered.status == STATUS_COMMITTED

    assert recovered.candidate_checkpoint == {
        "value": "2026-10-07T03:00:00Z",
    }

    assert (
        recovered.checkpoint_after
        == after
    )


def test_page_three_failure_has_no_partial_bronze_and_recovery_commits_once():
    before = _checkpoint(
        "2026-10-07T01:00:00Z",
        version=7,
    )

    expected_batch = logical_batch_id(
        DATASET,
        before,
    )

    config = get_dataset_config(DATASET)

    after = CheckpointState(
        dataset_id=DATASET,
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-07T03:00:00Z",
        },
        version=8,
        last_batch_id=expected_batch,
    )

    store = Mock()
    store.load.return_value = before
    store.compare_and_set.return_value = after

    page_events = []
    attempt = {
        "number": 0,
    }

    def fetch_pages(
        *_args,
        **_kwargs,
    ):
        attempt["number"] += 1

        if attempt["number"] == 1:
            page_events.extend([
                "run1_page1_ok",
                "run1_page2_ok",
                "run1_page3_fail",
            ])

            raise RuntimeError(
                "simulated page 3 failure"
            )

        page_events.extend([
            "run2_page1_ok",
            "run2_page2_ok",
            "run2_page3_ok",
        ])

        return {
            "data": [
                {
                    "record_id": "r1",
                    config.source_updated_at_field:
                        "2026-10-07T01:30:00Z",
                },
                {
                    "record_id": "r2",
                    config.source_updated_at_field:
                        "2026-10-07T02:00:00Z",
                },
                {
                    "record_id": "r3",
                    config.source_updated_at_field:
                        "2026-10-07T03:00:00Z",
                },
            ],
        }

    bronze = Mock(
        return_value=_recovery_bronze_result(
            config,
            batch_id=expected_batch,
            count=3,
        )
    )

    # ------------------------------------------
    # First run:
    # page 1 OK, page 2 OK, page 3 FAIL.
    # ------------------------------------------

    try:
        run_incremental_ingestion(
            Mock(),
            "http://mock/api",
            dataset=DATASET,
            checkpoint_store=store,
            fetch_payload=fetch_pages,
            ingest_payload=bronze,
        )
    except RuntimeError as exc:
        assert (
            str(exc)
            == "simulated page 3 failure"
        )
    else:
        raise AssertionError(
            "first run must fail on page 3"
        )

    assert page_events == [
        "run1_page1_ok",
        "run1_page2_ok",
        "run1_page3_fail",
    ]

    # No partial Bronze is allowed.
    bronze.assert_not_called()

    # Checkpoint must remain untouched.
    store.compare_and_set.assert_not_called()

    # ------------------------------------------
    # Recovery run:
    # all three pages succeed.
    # ------------------------------------------

    recovered = run_incremental_ingestion(
        Mock(),
        "http://mock/api",
        dataset=DATASET,
        checkpoint_store=store,
        fetch_payload=fetch_pages,
        ingest_payload=bronze,
    )

    assert page_events == [
        "run1_page1_ok",
        "run1_page2_ok",
        "run1_page3_fail",
        "run2_page1_ok",
        "run2_page2_ok",
        "run2_page3_ok",
    ]

    # Bronze is written exactly once:
    # only after the complete recovery fetch.
    assert bronze.call_count == 1

    bronze_kwargs = (
        bronze.call_args.kwargs
    )

    assert (
        bronze_kwargs["batch_id"]
        == expected_batch
    )

    assert (
        bronze_kwargs["expected_count"]
        == 3
    )

    # Checkpoint advances exactly once,
    # after the successful Bronze write.
    assert store.compare_and_set.call_count == 1

    cas_kwargs = (
        store.compare_and_set.call_args.kwargs
    )

    assert (
        cas_kwargs["batch_id"]
        == expected_batch
    )

    assert cas_kwargs["checkpoint_payload"] == {
        "value": "2026-10-07T03:00:00Z",
    }

    assert recovered.status == STATUS_COMMITTED
    assert recovered.batch_id == expected_batch
    assert recovered.fetched_count == 3
    assert recovered.checkpoint_before == before
    assert recovered.checkpoint_after == after


def test_pm_replay_100_then_plus_20_ends_with_120_total_bronze_rows():
    config = get_dataset_config(DATASET)

    class MemoryCheckpointStore:
        def __init__(self):
            self.state = None

        def load(self, dataset_id):
            assert dataset_id == DATASET
            return self.state

        def compare_and_set(
            self,
            *,
            dataset_id,
            strategy_type,
            expected_version,
            checkpoint_payload,
            batch_id,
        ):
            assert dataset_id == DATASET

            current_version = (
                self.state.version
                if self.state is not None
                else None
            )

            assert (
                current_version
                == expected_version
            )

            next_version = (
                1
                if self.state is None
                else self.state.version + 1
            )

            self.state = CheckpointState(
                dataset_id=dataset_id,
                strategy_type=strategy_type,
                checkpoint_payload=dict(
                    checkpoint_payload
                ),
                version=next_version,
                last_batch_id=batch_id,
            )

            return self.state

    store = MemoryCheckpointStore()

    source_rows = []

    for index in range(100):
        source_rows.append({
            "record_id": f"base-{index:03d}",
            config.source_updated_at_field:
                "2026-10-07T01:00:00Z",
        })

    bronze_objects = {}

    def fetch_source(
        _api_url,
        *,
        updated_after,
        **_kwargs,
    ):
        if updated_after is None:
            rows = list(source_rows)
        else:
            rows = [
                dict(row)
                for row in source_rows
                if (
                    row[
                        config.source_updated_at_field
                    ]
                    > updated_after
                )
            ]

        return {
            "data": rows,
        }

    def write_bronze(
        _spark,
        payload,
        *,
        batch_id,
        expected_count,
        **_kwargs,
    ):
        object_key = (
            config.bronze_prefix
            + "batch_id="
            + batch_id
            + "/data.parquet"
        )

        rows = [
            dict(row)
            for row in payload["data"]
        ]

        assert len(rows) == expected_count

        # Same object key means replay replaces
        # that logical batch instead of adding
        # another copy.
        bronze_objects[object_key] = rows

        return IngestionResult(
            object_key=object_key,
            input_count=len(rows),
            dataframe_count=len(rows),
            readback_count=len(rows),
            batch_id=batch_id,
            sample_record_id=(
                rows[0]["record_id"]
            ),
            sample_checksum="checksum",
        )

    def bronze_total():
        return sum(
            len(rows)
            for rows in bronze_objects.values()
        )

    # ------------------------------------------
    # Run 1:
    # source = 100
    # Bronze total must become 100.
    # ------------------------------------------

    first = run_incremental_ingestion(
        Mock(),
        "http://mock/api",
        dataset=DATASET,
        checkpoint_store=store,
        fetch_payload=fetch_source,
        ingest_payload=write_bronze,
    )

    assert first.status == STATUS_COMMITTED
    assert first.fetched_count == 100

    assert len(bronze_objects) == 1
    assert bronze_total() == 100

    first_batch = first.batch_id

    assert store.state is not None
    assert store.state.version == 1
    assert store.state.checkpoint_payload == {
        "value": "2026-10-07T01:00:00Z",
    }

    # ------------------------------------------
    # Replay with unchanged source:
    # F3 sees no records newer than checkpoint.
    # Therefore NO_CHANGE and Bronze stays 100.
    # ------------------------------------------

    replay = run_incremental_ingestion(
        Mock(),
        "http://mock/api",
        dataset=DATASET,
        checkpoint_store=store,
        fetch_payload=fetch_source,
        ingest_payload=write_bronze,
    )

    assert replay.status == "NO_CHANGE"
    assert replay.fetched_count == 0

    assert len(bronze_objects) == 1
    assert bronze_total() == 100

    # NO_CHANGE must not advance checkpoint.
    assert store.state.version == 1

    # ------------------------------------------
    # Source receives 20 new records.
    # ------------------------------------------

    for index in range(20):
        source_rows.append({
            "record_id": f"new-{index:03d}",
            config.source_updated_at_field:
                "2026-10-07T02:00:00Z",
        })

    # ------------------------------------------
    # Next incremental run:
    # only 20 new rows are written.
    # Bronze total becomes 120.
    # ------------------------------------------

    next_run = run_incremental_ingestion(
        Mock(),
        "http://mock/api",
        dataset=DATASET,
        checkpoint_store=store,
        fetch_payload=fetch_source,
        ingest_payload=write_bronze,
    )

    assert next_run.status == STATUS_COMMITTED
    assert next_run.fetched_count == 20

    assert len(bronze_objects) == 2
    assert bronze_total() == 120

    assert next_run.batch_id != first_batch

    assert store.state.version == 2
    assert store.state.last_batch_id == next_run.batch_id

    assert store.state.checkpoint_payload == {
        "value": "2026-10-07T02:00:00Z",
    }
