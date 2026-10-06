# -*- coding: utf-8 -*-
"""Reusable HTTP transport adapter.

This module owns HTTP GET transport, bearer authentication,
timeout handling and cursor pagination.

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


PayloadValidator = Callable[
    [dict[str, Any]],
    None,
]


class HttpAdapter:
    """Fetch cursor-paginated JSON payloads over HTTP GET."""

    def __init__(
        self,
        api_url: str,
        *,
        api_key: str | None = None,
        page_limit: int = 500,
        timeout_seconds: int = 30,
        updated_after: str | None = None,
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
        seen_cursors: set[str] = set()

        first_page: (
            dict[str, Any] | None
        ) = None

        cursor: str | None = None
        page_number = 0

        while True:

            page_number += 1

            if page_number > 10000:
                raise RuntimeError(
                    "Pagination exceeded safety limit"
                )

            query = dict(
                base_query
            )

            query["limit"] = str(
                self.page_limit
            )

            if self.updated_after:
                query["updated_after"] = (
                    self.updated_after
                )

            if cursor:
                query["cursor"] = cursor
            else:
                query.pop(
                    "cursor",
                    None,
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

            pagination = payload[
                "pagination"
            ]

            print(
                "HTTP_PAGE="
                f"{page_number}"
                f"|records={len(page_records)}"
                f"|has_more="
                f"{pagination['has_more']}"
            )

            if not pagination[
                "has_more"
            ]:
                break

            next_cursor = pagination.get(
                "next_cursor"
            )

            if not next_cursor:
                raise ValueError(
                    "API says has_more=true "
                    "but next_cursor is missing"
                )

            if (
                next_cursor
                in seen_cursors
            ):
                raise ValueError(
                    "Repeated pagination "
                    "cursor detected"
                )

            seen_cursors.add(
                next_cursor
            )

            cursor = next_cursor

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
