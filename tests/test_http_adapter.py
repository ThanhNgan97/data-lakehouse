from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import call, patch
from urllib.error import HTTPError, URLError


ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0,
    str(ROOT / "lakehouse" / "spark"),
)

from http_adapter import HttpAdapter


class FakeResponse(io.BytesIO):

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type,
        exc,
        tb,
    ):
        self.close()


def response(payload):
    return FakeResponse(
        json.dumps(payload).encode(
            "utf-8"
        )
    )

def http_error(
    status_code,
    *,
    body=b"failure",
    headers=None,
):
    return HTTPError(
        "http://example.test/data",
        status_code,
        "test error",
        hdrs=headers,
        fp=io.BytesIO(body),
    )


def page(
    records,
    *,
    has_more=False,
    next_cursor=None,
):
    return {
        "source_system": "test_source",
        "dataset": "test.dataset",
        "schema_version": "1",
        "data": records,
        "pagination": {
            "returned_records":
                len(records),
            "has_more":
                has_more,
            "next_cursor":
                next_cursor,
        },
    }


class HttpAdapterTests(
    unittest.TestCase
):

    def test_http_200_returns_records(self):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            return_value=response(
                page(
                    [{"id": "1"}]
                )
            ),
        ):

            payload = adapter.fetch(
                record_id_field="id",
            )

        self.assertEqual(
            payload["data"],
            [{"id": "1"}],
        )

    def test_non_retryable_http_statuses_fail_without_retry(
        self,
    ):

        for status_code in (
            401,
            403,
            404,
        ):

            with self.subTest(
                status_code=status_code
            ):

                adapter = HttpAdapter(
                    "http://example.test/data"
                )

                with patch(
                    "http_adapter.urlopen",
                    side_effect=http_error(
                        status_code
                    ),
                ) as mock_urlopen:

                    with patch(
                        "http_adapter.time.sleep"
                    ) as mock_sleep:

                        with self.assertRaisesRegex(
                            RuntimeError,
                            f"status={status_code}",
                        ):
                            adapter.fetch()

                self.assertEqual(
                    mock_urlopen.call_count,
                    1,
                )

                mock_sleep.assert_not_called()

    def test_429_honors_retry_after_then_succeeds(
        self,
    ):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=[
                http_error(
                    429,
                    headers={
                        "Retry-After": "4"
                    },
                ),
                response(
                    page(
                        [{"id": "1"}]
                    )
                ),
            ],
        ) as mock_urlopen:

            with patch(
                "http_adapter.time.sleep"
            ) as mock_sleep:

                payload = adapter.fetch(
                    record_id_field="id"
                )

        self.assertEqual(
            payload["data"],
            [{"id": "1"}],
        )

        self.assertEqual(
            mock_urlopen.call_count,
            2,
        )

        mock_sleep.assert_called_once_with(
            4.0
        )

    def test_429_without_retry_after_uses_backoff(
        self,
    ):

        adapter = HttpAdapter(
            "http://example.test/data",
            backoff_seconds=1.0,
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=[
                http_error(429),
                response(
                    page(
                        [{"id": "1"}]
                    )
                ),
            ],
        ):

            with patch(
                "http_adapter.time.sleep"
            ) as mock_sleep:

                payload = adapter.fetch()

        self.assertEqual(
            payload["data"],
            [{"id": "1"}],
        )

        mock_sleep.assert_called_once_with(
            1.0
        )

    def test_503_retries_with_exponential_backoff(
        self,
    ):

        adapter = HttpAdapter(
            "http://example.test/data",
            max_attempts=3,
            backoff_seconds=1.0,
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=[
                http_error(503),
                http_error(503),
                response(
                    page(
                        [{"id": "1"}]
                    )
                ),
            ],
        ) as mock_urlopen:

            with patch(
                "http_adapter.time.sleep"
            ) as mock_sleep:

                payload = adapter.fetch()

        self.assertEqual(
            payload["data"],
            [{"id": "1"}],
        )

        self.assertEqual(
            mock_urlopen.call_count,
            3,
        )

        mock_sleep.assert_has_calls(
            [
                call(1.0),
                call(2.0),
            ]
        )

    def test_503_fails_after_retry_budget_exhausted(
        self,
    ):

        adapter = HttpAdapter(
            "http://example.test/data",
            max_attempts=3,
            backoff_seconds=1.0,
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=[
                http_error(503),
                http_error(503),
                http_error(503),
            ],
        ) as mock_urlopen:

            with patch(
                "http_adapter.time.sleep"
            ) as mock_sleep:

                with self.assertRaisesRegex(
                    RuntimeError,
                    "retry budget exhausted",
                ):
                    adapter.fetch()

        self.assertEqual(
            mock_urlopen.call_count,
            3,
        )

        mock_sleep.assert_has_calls(
            [
                call(1.0),
                call(2.0),
            ]
        )

        self.assertEqual(
            mock_sleep.call_count,
            2,
        )

    def test_invalid_json_surfaces_failure_without_retry(
        self,
    ):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            return_value=FakeResponse(
                b"{not-json"
            ),
        ) as mock_urlopen:

            with patch(
                "http_adapter.time.sleep"
            ) as mock_sleep:

                with self.assertRaisesRegex(
                    ValueError,
                    "malformed JSON",
                ):
                    adapter.fetch()

        self.assertEqual(
            mock_urlopen.call_count,
            1,
        )

        mock_sleep.assert_not_called()

    def test_empty_response_preserved(self):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            return_value=response(
                page([])
            ),
        ):

            payload = adapter.fetch()

        self.assertEqual(
            payload["data"],
            [],
        )

    def test_timeout_retries_then_succeeds(
        self,
    ):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=[
                URLError("timed out"),
                response(
                    page(
                        [{"id": "1"}]
                    )
                ),
            ],
        ) as mock_urlopen:

            with patch(
                "http_adapter.time.sleep"
            ) as mock_sleep:

                payload = adapter.fetch()

        self.assertEqual(
            payload["data"],
            [{"id": "1"}],
        )

        self.assertEqual(
            mock_urlopen.call_count,
            2,
        )

        mock_sleep.assert_called_once_with(
            1.0
        )

    def test_connection_reset_retries_then_succeeds(
        self,
    ):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=[
                ConnectionResetError(
                    "connection reset by peer"
                ),
                response(
                    page(
                        [{"id": "1"}]
                    )
                ),
            ],
        ) as mock_urlopen:

            with patch(
                "http_adapter.time.sleep"
            ) as mock_sleep:

                payload = adapter.fetch()

        self.assertEqual(
            payload["data"],
            [{"id": "1"}],
        )

        self.assertEqual(
            mock_urlopen.call_count,
            2,
        )

        mock_sleep.assert_called_once_with(
            1.0
        )

    def test_cursor_pagination(self):

        adapter = HttpAdapter(
            "http://example.test/data",
            page_limit=1,
        )

        first = page(
            [{"id": "1"}],
            has_more=True,
            next_cursor="cursor-2",
        )

        second = page(
            [{"id": "2"}],
            has_more=False,
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=[
                response(first),
                response(second),
            ],
        ) as mock_urlopen:

            payload = adapter.fetch(
                record_id_field="id",
                consistency_fields=(
                    "source_system",
                    "dataset",
                    "schema_version",
                ),
            )

        self.assertEqual(
            payload["data"],
            [
                {"id": "1"},
                {"id": "2"},
            ],
        )

        self.assertEqual(
            mock_urlopen.call_count,
            2,
        )


if __name__ == "__main__":
    unittest.main()
