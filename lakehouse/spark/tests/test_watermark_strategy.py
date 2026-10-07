import pytest

from watermark_strategy import (
    TimestampWatermarkStrategy,
    resolve_watermark_strategy,
)


def test_first_run_has_no_request_watermark():
    strategy = TimestampWatermarkStrategy()

    assert strategy.request_watermark(None) is None


def test_request_watermark_normalizes_to_utc():
    strategy = TimestampWatermarkStrategy()

    value = strategy.request_watermark(
        {"value": "2026-10-06T14:00:00+07:00"}
    )

    assert value == "2026-10-06T07:00:00Z"


def test_candidate_uses_max_timestamp_not_record_order():
    strategy = TimestampWatermarkStrategy()

    candidate = strategy.candidate(
        [
            {"updated_at": "2026-10-06T10:00:00Z"},
            {"updated_at": "2026-10-06T09:00:00Z"},
            {"updated_at": "2026-10-06T18:00:00+07:00"},
        ],
        field_name="updated_at",
    )

    assert candidate == {
        "value": "2026-10-06T11:00:00Z",
    }


def test_candidate_empty_records_returns_none():
    strategy = TimestampWatermarkStrategy()

    assert strategy.candidate(
        [],
        field_name="updated_at",
    ) is None


def test_candidate_rejects_missing_watermark_field():
    strategy = TimestampWatermarkStrategy()

    with pytest.raises(
        ValueError,
        match="missing watermark field",
    ):
        strategy.candidate(
            [{"record_id": "r1"}],
            field_name="updated_at",
        )


def test_candidate_rejects_naive_timestamp():
    strategy = TimestampWatermarkStrategy()

    with pytest.raises(
        ValueError,
        match="timezone offset",
    ):
        strategy.candidate(
            [{"updated_at": "2026-10-06T10:00:00"}],
            field_name="updated_at",
        )


def test_stored_payload_must_have_exact_shape():
    strategy = TimestampWatermarkStrategy()

    with pytest.raises(
        ValueError,
        match="exactly 'value'",
    ):
        strategy.request_watermark(
            {
                "value": "2026-10-06T10:00:00Z",
                "extra": "unexpected",
            }
        )


def test_first_candidate_is_valid_without_previous_checkpoint():
    strategy = TimestampWatermarkStrategy()

    strategy.validate_advance(
        None,
        {"value": "2026-10-06T10:00:00Z"},
    )


def test_candidate_must_advance_previous_checkpoint():
    strategy = TimestampWatermarkStrategy()

    with pytest.raises(
        ValueError,
        match="greater than",
    ):
        strategy.validate_advance(
            {"value": "2026-10-06T10:00:00Z"},
            {"value": "2026-10-06T10:00:00Z"},
        )


def test_candidate_older_than_previous_checkpoint_is_rejected():
    strategy = TimestampWatermarkStrategy()

    with pytest.raises(
        ValueError,
        match="greater than",
    ):
        strategy.validate_advance(
            {"value": "2026-10-06T10:00:00Z"},
            {"value": "2026-10-06T09:59:59Z"},
        )


def test_candidate_later_than_previous_checkpoint_is_valid():
    strategy = TimestampWatermarkStrategy()

    strategy.validate_advance(
        {"value": "2026-10-06T10:00:00Z"},
        {"value": "2026-10-06T10:00:01Z"},
    )


def test_resolver_supports_timestamp():
    strategy = resolve_watermark_strategy("timestamp")

    assert isinstance(
        strategy,
        TimestampWatermarkStrategy,
    )


def test_resolver_rejects_unknown_strategy():
    with pytest.raises(
        ValueError,
        match="Unsupported watermark strategy",
    ):
        resolve_watermark_strategy("source_cursor")
