# -*- coding: utf-8 -*-
"""Reusable pagination strategies for HTTP API ingestion.

This module owns pagination state transitions and request parameters.
It intentionally knows nothing about HTTP retry/backoff, dataset
business fields, checkpoint persistence, Bronze, Silver, or Gold.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, TypeAlias


PaginationState: TypeAlias = str | int | None
PaginationQueryParams: TypeAlias = dict[str, str | None]


@dataclass(frozen=True)
class PaginationDecision:
    """Result of processing one successfully fetched API page."""

    done: bool
    next_state: PaginationState = None


def _pagination_metadata(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Return validated canonical pagination metadata."""

    pagination = payload.get("pagination")

    if not isinstance(pagination, dict):
        raise ValueError(
            "API pagination metadata must be a JSON object"
        )

    has_more = pagination.get("has_more")

    if not isinstance(has_more, bool):
        raise ValueError(
            "API pagination.has_more must be a boolean"
        )

    return pagination, has_more


class PaginationStrategy(ABC):
    """Common interface for one pagination mechanism."""

    @abstractmethod
    def initial_state(self) -> PaginationState:
        """Return the state used for the first request."""

    @abstractmethod
    def request_params(
        self,
        state: PaginationState,
        *,
        limit: int,
    ) -> PaginationQueryParams:
        """Return pagination query parameters for the current state."""

    @abstractmethod
    def advance(
        self,
        state: PaginationState,
        payload: dict[str, Any],
        *,
        limit: int,
    ) -> PaginationDecision:
        """Return whether pagination is done and the next state."""


class CursorPagination(PaginationStrategy):
    """Pagination driven by an opaque server-provided cursor."""

    def initial_state(self) -> PaginationState:
        return None

    def request_params(
        self,
        state: PaginationState,
        *,
        limit: int,
    ) -> PaginationQueryParams:

        if (
            state is not None
            and not isinstance(state, str)
        ):
            raise ValueError(
                "Cursor pagination state must be a string or None"
            )

        return {
            "limit": str(limit),
            "cursor": state,
        }

    def advance(
        self,
        state: PaginationState,
        payload: dict[str, Any],
        *,
        limit: int,
    ) -> PaginationDecision:

        pagination, has_more = (
            _pagination_metadata(payload)
        )

        if not has_more:
            return PaginationDecision(
                done=True,
            )

        next_cursor = pagination.get(
            "next_cursor"
        )

        if (
            not isinstance(next_cursor, str)
            or not next_cursor
        ):
            raise ValueError(
                "API says has_more=true "
                "but next_cursor is missing"
            )

        return PaginationDecision(
            done=False,
            next_state=next_cursor,
        )


class PageLimitPagination(PaginationStrategy):
    """Pagination using one-based page and limit parameters."""

    def initial_state(self) -> PaginationState:
        return 1

    def request_params(
        self,
        state: PaginationState,
        *,
        limit: int,
    ) -> PaginationQueryParams:

        if (
            not isinstance(state, int)
            or isinstance(state, bool)
            or state < 1
        ):
            raise ValueError(
                "Page pagination state must be an integer >= 1"
            )

        return {
            "page": str(state),
            "limit": str(limit),
        }

    def advance(
        self,
        state: PaginationState,
        payload: dict[str, Any],
        *,
        limit: int,
    ) -> PaginationDecision:

        if (
            not isinstance(state, int)
            or isinstance(state, bool)
            or state < 1
        ):
            raise ValueError(
                "Page pagination state must be an integer >= 1"
            )

        _, has_more = _pagination_metadata(
            payload
        )

        if not has_more:
            return PaginationDecision(
                done=True,
            )

        return PaginationDecision(
            done=False,
            next_state=state + 1,
        )


class OffsetLimitPagination(PaginationStrategy):
    """Pagination using zero-based offset and limit parameters."""

    def initial_state(self) -> PaginationState:
        return 0

    def request_params(
        self,
        state: PaginationState,
        *,
        limit: int,
    ) -> PaginationQueryParams:

        if (
            not isinstance(state, int)
            or isinstance(state, bool)
            or state < 0
        ):
            raise ValueError(
                "Offset pagination state must be an integer >= 0"
            )

        return {
            "offset": str(state),
            "limit": str(limit),
        }

    def advance(
        self,
        state: PaginationState,
        payload: dict[str, Any],
        *,
        limit: int,
    ) -> PaginationDecision:

        if (
            not isinstance(state, int)
            or isinstance(state, bool)
            or state < 0
        ):
            raise ValueError(
                "Offset pagination state must be an integer >= 0"
            )

        _, has_more = _pagination_metadata(
            payload
        )

        if not has_more:
            return PaginationDecision(
                done=True,
            )

        return PaginationDecision(
            done=False,
            next_state=state + limit,
        )
