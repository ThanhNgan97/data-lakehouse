from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
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

    def test_non_2xx_controlled_failure(self):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        error = HTTPError(
            "http://example.test/data",
            500,
            "boom",
            hdrs=None,
            fp=io.BytesIO(b"failure"),
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=error,
        ):

            with self.assertRaisesRegex(
                RuntimeError,
                "status=500",
            ):
                adapter.fetch()

    def test_invalid_json_surfaces_failure(self):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            return_value=FakeResponse(
                b"{not-json"
            ),
        ):

            with self.assertRaises(
                ValueError
            ):
                adapter.fetch()

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

    def test_timeout_connection_failure(self):

        adapter = HttpAdapter(
            "http://example.test/data"
        )

        with patch(
            "http_adapter.urlopen",
            side_effect=URLError(
                "timed out"
            ),
        ):

            with self.assertRaisesRegex(
                RuntimeError,
                "connection failed",
            ):
                adapter.fetch()

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
        ):

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


if __name__ == "__main__":
    unittest.main()
