"""Watermark strategies for incremental API ingestion.

A watermark strategy interprets checkpoint payloads and derives the next
candidate checkpoint from source records. It has no database, Spark, Airflow,
HTTP, or Bronze persistence responsibilities.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any


CheckpointPayload = Mapping[str, Any]


def _parse_aware_datetime(
    value: Any,
    *,
    field_name: str,
) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"{field_name} must be a non-empty ISO-8601 string"
        )

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError(
            f"{field_name} is not a valid ISO-8601 datetime: "
            f"{value!r}"
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError(
            f"{field_name} must contain a timezone offset: "
            f"{value!r}"
        )

    return parsed.astimezone(timezone.utc)


def _serialize_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


class WatermarkStrategy(ABC):
    """Interpret and advance one kind of checkpoint payload."""

    strategy_type: str

    @abstractmethod
    def request_watermark(
        self,
        checkpoint_payload: CheckpointPayload | None,
    ) -> str | None:
        """Return the source lower bound sent as updated_after."""

    @abstractmethod
    def candidate(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        field_name: str,
    ) -> dict[str, Any] | None:
        """Derive a candidate checkpoint from fetched source records."""

    @abstractmethod
    def validate_advance(
        self,
        previous_payload: CheckpointPayload | None,
        candidate_payload: CheckpointPayload,
    ) -> None:
        """Reject a candidate that does not safely advance state."""


class TimestampWatermarkStrategy(WatermarkStrategy):
    """Timestamp checkpoint with payload shape: {"value": "<ISO-8601>"}."""

    strategy_type = "timestamp"

    @staticmethod
    def _payload_datetime(
        payload: CheckpointPayload,
        *,
        label: str,
    ) -> datetime:
        if not isinstance(payload, Mapping):
            raise ValueError(
                f"{label} checkpoint payload must be an object"
            )

        if set(payload) != {"value"}:
            raise ValueError(
                f"{label} checkpoint payload must contain exactly "
                "'value'"
            )

        return _parse_aware_datetime(
            payload["value"],
            field_name=f"{label} checkpoint value",
        )

    def request_watermark(
        self,
        checkpoint_payload: CheckpointPayload | None,
    ) -> str | None:
        if checkpoint_payload is None:
            return None

        value = self._payload_datetime(
            checkpoint_payload,
            label="stored",
        )

        return _serialize_utc(value)

    def candidate(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        field_name: str,
    ) -> dict[str, Any] | None:
        if not field_name:
            raise ValueError(
                "source updated-at field name must not be empty"
            )

        if not records:
            return None

        timestamps: list[datetime] = []

        for index, record in enumerate(records):
            if not isinstance(record, Mapping):
                raise ValueError(
                    f"record[{index}] must be an object"
                )

            if field_name not in record:
                raise ValueError(
                    f"record[{index}] is missing watermark field "
                    f"{field_name!r}"
                )

            timestamps.append(
                _parse_aware_datetime(
                    record[field_name],
                    field_name=f"record[{index}].{field_name}",
                )
            )

        maximum = max(timestamps)

        return {
            "value": _serialize_utc(maximum),
        }

    def validate_advance(
        self,
        previous_payload: CheckpointPayload | None,
        candidate_payload: CheckpointPayload,
    ) -> None:
        candidate = self._payload_datetime(
            candidate_payload,
            label="candidate",
        )

        if previous_payload is None:
            return

        previous = self._payload_datetime(
            previous_payload,
            label="stored",
        )

        if candidate <= previous:
            raise ValueError(
                "candidate timestamp must be greater than "
                "stored checkpoint"
            )


def resolve_watermark_strategy(
    strategy_type: str,
) -> WatermarkStrategy:
    if strategy_type == TimestampWatermarkStrategy.strategy_type:
        return TimestampWatermarkStrategy()

    raise ValueError(
        f"Unsupported watermark strategy: {strategy_type!r}"
    )
