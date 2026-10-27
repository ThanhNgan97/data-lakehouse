from __future__ import annotations

import io
import json
import socket
import unittest
from urllib.error import HTTPError

import gx_airflow_adapter as adapter


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
        return False


def success():
    return {
        "status": "PASSED",
        "validation_id": "validation-1",
        "validation_uri": "s3://bucket/result.json",
        "merge_allowed": True,
        "approval": {
            "validated_branch": "candidate",
            "validated_snapshot_id": "1",
            "validated_commit_hash": "abc",
        },
        "large_summary_that_must_not_enter_xcom": "x" * 10000,
    }


def http_error(payload, status=409):
    return HTTPError(
        "http://gx/v1/validate/table",
        status,
        "error",
        {},
        io.BytesIO(json.dumps(payload).encode("utf-8")),
    )


class GXAirflowAdapterTest(unittest.TestCase):
    def test_success_returns_bounded_reference_not_full_summary(self):
        result = adapter.execute_quality_gate(
            {"snapshot_id": "1"},
            service_url="http://gx",
            opener=lambda *_args, **_kwargs: Response(
                json.dumps(success()).encode("utf-8")
            ),
        )
        self.assertLessEqual(len(json.dumps(result).encode("utf-8")), 4096)
        self.assertNotIn("large_summary_that_must_not_enter_xcom", result)
        self.assertEqual(result["validation_uri"], "s3://bucket/result.json")

    def test_snapshot_mismatch_refreshes_once_then_revalidates(self):
        requests = []

        def opener(call, **_kwargs):
            requests.append(json.loads(call.data))
            if len(requests) == 1:
                raise http_error(
                    {
                        "error_code": "SNAPSHOT_MISMATCH",
                        "message": "changed",
                        "retryable": True,
                        "current_identity": {
                            "nessie_branch": "candidate",
                            "snapshot_id": "2",
                            "nessie_commit_hash": "def",
                        },
                    }
                )
            return Response(json.dumps(success()).encode("utf-8"))

        adapter.execute_quality_gate(
            {"snapshot_id": "1", "nessie_commit_hash": "abc"},
            service_url="http://gx",
            opener=opener,
        )
        self.assertEqual(requests[1]["snapshot_id"], "2")
        self.assertEqual(requests[1]["nessie_commit_hash"], "def")

    def test_quality_and_configuration_failures_are_non_retryable(self):
        for code in ("DATA_QUALITY_FAILED", "GX_CONFIGURATION_ERROR"):
            with self.subTest(code=code), self.assertRaises(adapter.GXAirflowError) as raised:
                adapter.execute_quality_gate(
                    {},
                    service_url="http://gx",
                    opener=lambda *_args, c=code, **_kwargs: (_ for _ in ()).throw(
                        http_error({"error_code": c, "message": "stop", "retryable": False}, 422)
                    ),
                )
            self.assertFalse(raised.exception.retryable)

    def test_timeout_has_distinct_retryable_category(self):
        with self.assertRaises(adapter.GXAirflowError) as raised:
            adapter.execute_quality_gate(
                {},
                service_url="http://gx",
                opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    socket.timeout("deadline")
                ),
            )
        self.assertEqual(raised.exception.code, "VALIDATION_TIMEOUT")
        self.assertTrue(raised.exception.retryable)


if __name__ == "__main__":
    unittest.main()
