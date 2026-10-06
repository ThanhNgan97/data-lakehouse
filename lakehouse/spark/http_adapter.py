# -*- coding: utf-8 -*-
"""Reusable HTTP transport adapter.

This module owns HTTP GET transport, bearer authentication,
timeout handling and pagination orchestration.

It intentionally knows nothing about Learning Outcomes business
fields, Silver keys, Gold metrics, Nessie or Superset.
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlsplit,
    urlunsplit,
)
from urllib.request import Request, urlopen

from pagination import (
    CursorPagination,
    PaginationState,
    PaginationStrategy,
)


PayloadValidator = Callable[
    [dict[str, Any]],
    None,
]


class HttpAdapter:
    """Fetch paginated JSON payloads over HTTP GET."""

    def __init__(
        self,
        api_url: str,
        *,
        api_key: str | None = None,
        page_limit: int = 500,
        timeout_seconds: int = 30,
        updated_after: str | None = None,
        pagination_strategy: PaginationStrategy | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:

        if not api_url:
            raise ValueError(
                "api_url must not be empty"
            )

        if (
            page_limit < 1
            or page_limit > 500
        ):
            raise ValueError(
                "page_limit must be between 1 and 500"
            )

        if timeout_seconds < 1:
            raise ValueError(
                "timeout_seconds must be >= 1"
            )

        if max_attempts < 1:
            raise ValueError(
                "max_attempts must be >= 1"
            )

        if backoff_seconds < 0:
            raise ValueError(
                "backoff_seconds must be >= 0"
            )

        self.api_url = api_url
        self.api_key = api_key
        self.page_limit = page_limit
        self.timeout_seconds = timeout_seconds
        self.updated_after = updated_after
        self.pagination_strategy = (
            pagination_strategy
            if pagination_strategy is not None
            else CursorPagination()
        )
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds

    @staticmethod
    def _is_retryable_http_status(
        status_code: int,
    ) -> bool:
        return (
            status_code == 429
            or 500 <= status_code <= 599
        )

    def _backoff_delay(
        self,
        attempt: int,
    ) -> float:
        return self.backoff_seconds * (
            2 ** (attempt - 1)
        )

    def _retry_delay(
        self,
        attempt: int,
        retry_after: str | None = None,
    ) -> float:

        if retry_after is not None:
            try:
                delay = float(retry_after)

                if delay >= 0:
                    return delay

            except ValueError:
                pass

        return self._backoff_delay(
            attempt
        )

    def _request_json(
        self,
        request: Request,
    ) -> Any:

        for attempt in range(
            1,
            self.max_attempts + 1,
        ):

            try:

                with urlopen(
                    request,
                    timeout=self.timeout_seconds,
                ) as response:

                    try:
                        payload = json.load(
                            response
                        )

                    except json.JSONDecodeError as exc:

                        raise ValueError(
                            "HTTP API returned "
                            "malformed JSON: "
                            f"{exc.msg}"
                        ) from exc

                    return payload

            except HTTPError as exc:

                status_code = exc.code
                headers = exc.headers

                try:
                    body = (
                        exc.read()
                        .decode(
                            "utf-8",
                            errors="replace",
                        )
                    )
                except Exception:
                    body = ""
                finally:
                    exc.close()

                if not self._is_retryable_http_status(
                    status_code
                ):
                    raise RuntimeError(
                        "HTTP API returned "
                        f"non-retryable status="
                        f"{status_code}: {body}"
                    ) from exc

                if attempt >= self.max_attempts:
                    raise RuntimeError(
                        "HTTP API retry budget "
                        "exhausted: "
                        f"status={status_code}, "
                        f"attempts={attempt}: "
                        f"{body}"
                    ) from exc

                retry_after = None

                if status_code == 429:
                    retry_after = (
                        headers.get(
                            "Retry-After"
                        )
                        if headers
                        else None
                    )

                delay = self._retry_delay(
                    attempt,
                    retry_after,
                )

                time.sleep(delay)

            except (
                URLError,
                TimeoutError,
                ConnectionError,
            ) as exc:

                if attempt >= self.max_attempts:
                    raise RuntimeError(
                        "HTTP API connection "
                        "retry budget exhausted: "
                        f"attempts={attempt}: "
                        f"{exc}"
                    ) from exc

                delay = self._backoff_delay(
                    attempt
                )

                time.sleep(delay)

        raise RuntimeError(
            "HTTP API request ended "
            "without a result"
        )

    def fetch(
        self,
        *,
        validate_payload: PayloadValidator | None = None,
        record_id_field: str | None = None,
        consistency_fields: tuple[str, ...] = (),
    ) -> dict[str, Any]:

        parts = urlsplit(
            self.api_url
        )

        base_query = dict(
            parse_qsl(
                parts.query,
                keep_blank_values=True,
            )
        )

        headers = {
            "Accept": "application/json",
        }

        if self.api_key:
            headers["Authorization"] = (
                f"Bearer {self.api_key}"
            )

        all_records: list[
            dict[str, Any]
        ] = []

        seen_record_ids: set[str] = set()

        first_page: (
            dict[str, Any] | None
        ) = None

        pagination_strategy = (
            self.pagination_strategy
        )
        pagination_state = (
            pagination_strategy.initial_state()
        )
        seen_pagination_states: set[
            PaginationState
        ] = set()

        page_number = 0

        while True:

            page_number += 1

            if page_number > 10000:
                raise RuntimeError(
                    "Pagination exceeded safety limit"
                )

            if (
                pagination_state
                in seen_pagination_states
            ):
                raise ValueError(
                    "Repeated pagination state detected"
                )

            seen_pagination_states.add(
                pagination_state
            )

            query = dict(
                base_query
            )

            if self.updated_after:
                query["updated_after"] = (
                    self.updated_after
                )

            pagination_params = (
                pagination_strategy.request_params(
                    pagination_state,
                    limit=self.page_limit,
                )
            )

            for (
                parameter_name,
                parameter_value,
            ) in pagination_params.items():

                if parameter_value is None:
                    query.pop(
                        parameter_name,
                        None,
                    )
                else:
                    query[parameter_name] = (
                        parameter_value
                    )

            request_url = urlunsplit(
                (
                    parts.scheme,
                    parts.netloc,
                    parts.path,
                    urlencode(query),
                    parts.fragment,
                )
            )

            request = Request(
                request_url,
                headers=headers,
                method="GET",
            )

            payload = self._request_json(
                request
            )

            if not isinstance(
                payload,
                dict,
            ):
                raise ValueError(
                    "Top-level API response "
                    "must be a JSON object"
                )

            if validate_payload:
                validate_payload(
                    payload
                )

            if first_page is None:

                first_page = payload

            else:

                for field in (
                    consistency_fields
                ):

                    if (
                        payload.get(field)
                        != first_page.get(field)
                    ):
                        raise ValueError(
                            "Inconsistent "
                            f"{field} across API pages"
                        )

            page_records = payload[
                "data"
            ]

            for record in page_records:

                if record_id_field:

                    source_record_id = (
                        record[
                            record_id_field
                        ]
                    )

                    if (
                        source_record_id
                        in seen_record_ids
                    ):
                        raise ValueError(
                            "Duplicate source identifier "
                            "across API pages: "
                            f"{record_id_field}="
                            f"{source_record_id}"
                        )

                    seen_record_ids.add(
                        source_record_id
                    )

                all_records.append(
                    record
                )

            decision = (
                pagination_strategy.advance(
                    pagination_state,
                    payload,
                    limit=self.page_limit,
                )
            )

            print(
                "HTTP_PAGE="
                f"{page_number}"
                f"|records={len(page_records)}"
                f"|has_more={not decision.done}"
            )

            if decision.done:
                break

            if (
                decision.next_state
                in seen_pagination_states
            ):
                raise ValueError(
                    "Repeated pagination state detected"
                )

            pagination_state = (
                decision.next_state
            )

        if first_page is None:
            raise RuntimeError(
                "API returned no response pages"
            )

        merged_payload = dict(
            first_page
        )

        merged_payload["data"] = (
            all_records
        )

        merged_payload[
            "pagination"
        ] = {
            "returned_records":
                len(all_records),
            "has_more": False,
            "next_cursor": None,
        }

        if validate_payload:
            validate_payload(
                merged_payload
            )

        print(
            "HTTP_FETCH_TOTAL="
            f"{len(all_records)}"
            f"|pages={page_number}"
        )

        return merged_payload
