import json
from unittest.mock import Mock

import pytest

from checkpoint_store import (
    CheckpointConflictError,
    CheckpointState,
    CheckpointStoreError,
    PostgresCheckpointStore,
)


def _store_with_row(row):
    cursor = Mock()
    cursor.fetchone.return_value = row

    connection = Mock()
    connection.cursor.return_value = cursor

    factory = Mock(
        return_value=connection
    )

    store = PostgresCheckpointStore(
        connection_factory=factory
    )

    return store, factory, connection, cursor


def test_load_missing_checkpoint_returns_none():
    store, _, connection, cursor = _store_with_row(None)

    result = store.load(
        "education.learning_outcomes"
    )

    assert result is None

    cursor.execute.assert_called_once()
    connection.close.assert_called_once()


def test_load_returns_checkpoint_state():
    row = (
        "education.learning_outcomes",
        "timestamp",
        {"value": "2026-10-06T10:00:00Z"},
        7,
        "batch-7",
    )

    store, _, _, _ = _store_with_row(row)

    state = store.load(
        "education.learning_outcomes"
    )

    assert state == CheckpointState(
        dataset_id="education.learning_outcomes",
        strategy_type="timestamp",
        checkpoint_payload={
            "value": "2026-10-06T10:00:00Z"
        },
        version=7,
        last_batch_id="batch-7",
    )


def test_load_rejects_malformed_stored_payload():
    row = (
        "education.learning_outcomes",
        "timestamp",
        "not-an-object",
        7,
        "batch-7",
    )

    store, _, _, _ = _store_with_row(row)

    with pytest.raises(
        CheckpointStoreError,
        match="checkpoint_payload",
    ):
        store.load(
            "education.learning_outcomes"
        )


def test_first_compare_and_set_inserts_version_one():
    row = (
        "education.learning_outcomes",
        "timestamp",
        {"value": "2026-10-06T10:00:00Z"},
        1,
        "batch-1",
    )

    store, _, connection, cursor = _store_with_row(row)

    state = store.compare_and_set(
        dataset_id="education.learning_outcomes",
        strategy_type="timestamp",
        expected_version=None,
        checkpoint_payload={
            "value": "2026-10-06T10:00:00Z"
        },
        batch_id="batch-1",
    )

    assert state.version == 1

    sql, params = cursor.execute.call_args.args

    assert "INSERT INTO" in sql
    assert "ON CONFLICT (dataset_id) DO NOTHING" in sql
    assert "VALUES (%s, %s, %s::jsonb, 1, %s)" in sql

    assert params[0] == "education.learning_outcomes"
    assert params[1] == "timestamp"
    assert json.loads(params[2]) == {
        "value": "2026-10-06T10:00:00Z"
    }
    assert params[3] == "batch-1"

    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_first_compare_and_set_conflict_does_not_commit():
    store, _, connection, _ = _store_with_row(None)

    with pytest.raises(
        CheckpointConflictError,
        match="compare-and-set conflict",
    ):
        store.compare_and_set(
            dataset_id="education.learning_outcomes",
            strategy_type="timestamp",
            expected_version=None,
            checkpoint_payload={
                "value": "2026-10-06T10:00:00Z"
            },
            batch_id="batch-1",
        )

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once()


def test_existing_compare_and_set_uses_version_guard():
    row = (
        "education.learning_outcomes",
        "timestamp",
        {"value": "2026-10-06T11:00:00Z"},
        8,
        "batch-8",
    )

    store, _, connection, cursor = _store_with_row(row)

    state = store.compare_and_set(
        dataset_id="education.learning_outcomes",
        strategy_type="timestamp",
        expected_version=7,
        checkpoint_payload={
            "value": "2026-10-06T11:00:00Z"
        },
        batch_id="batch-8",
    )

    assert state.version == 8

    sql, params = cursor.execute.call_args.args

    assert "UPDATE public.api_ingestion_checkpoints" in sql
    assert "version = version + 1" in sql
    assert "AND version = %s" in sql
    assert "AND strategy_type = %s" in sql

    assert json.loads(params[0]) == {
        "value": "2026-10-06T11:00:00Z"
    }
    assert params[1:] == (
        "batch-8",
        "education.learning_outcomes",
        7,
        "timestamp",
    )

    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_existing_compare_and_set_conflict_rolls_back():
    store, _, connection, _ = _store_with_row(None)

    with pytest.raises(
        CheckpointConflictError,
        match="compare-and-set conflict",
    ):
        store.compare_and_set(
            dataset_id="education.learning_outcomes",
            strategy_type="timestamp",
            expected_version=7,
            checkpoint_payload={
                "value": "2026-10-06T11:00:00Z"
            },
            batch_id="batch-8",
        )

    connection.commit.assert_not_called()
    connection.rollback.assert_called_once()


def test_invalid_expected_version_fails_before_connection():
    factory = Mock()

    store = PostgresCheckpointStore(
        connection_factory=factory
    )

    with pytest.raises(
        ValueError,
        match="expected_version",
    ):
        store.compare_and_set(
            dataset_id="education.learning_outcomes",
            strategy_type="timestamp",
            expected_version=0,
            checkpoint_payload={
                "value": "2026-10-06T11:00:00Z"
            },
            batch_id="batch-8",
        )

    factory.assert_not_called()


def test_non_object_payload_fails_before_connection():
    factory = Mock()

    store = PostgresCheckpointStore(
        connection_factory=factory
    )

    with pytest.raises(
        ValueError,
        match="checkpoint_payload",
    ):
        store.compare_and_set(
            dataset_id="education.learning_outcomes",
            strategy_type="timestamp",
            expected_version=None,
            checkpoint_payload="bad-payload",
            batch_id="batch-1",
        )

    factory.assert_not_called()
