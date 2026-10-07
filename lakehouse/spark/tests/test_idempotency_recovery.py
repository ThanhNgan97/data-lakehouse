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
