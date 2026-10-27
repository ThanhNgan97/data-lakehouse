"""Airflow-side GX HTTP adapter with bounded XCom and error taxonomy."""

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from typing import Any, Callable
from urllib import error, request


NON_RETRYABLE = frozenset({"DATA_QUALITY_FAILED", "GX_CONFIGURATION_ERROR"})
RETRYABLE = frozenset(
    {"GX_EXECUTION_ERROR", "VALIDATION_TIMEOUT", "SNAPSHOT_MISMATCH"}
)
MAX_XCOM_BYTES = 4096


@dataclass
class GXAirflowError(RuntimeError):
    code: str
    message: str
    retryable: bool

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


def _post_json(
    url: str,
    payload: dict[str, Any],
    timeout_seconds: float,
    opener: Callable[..., Any] = request.urlopen,
) -> dict[str, Any]:
    call = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with opener(call, timeout=timeout_seconds) as response:
            return json.load(response)
    except error.HTTPError as exc:
        try:
            body = json.loads(exc.read())
        except Exception:
            body = {}
        code = body.get("error_code", "GX_EXECUTION_ERROR")
        wrapped = GXAirflowError(
            code,
            body.get("message", str(exc)),
            bool(body.get("retryable", code in RETRYABLE)),
        )
        wrapped.current_identity = body.get("current_identity")
        raise wrapped from exc
    except (TimeoutError, socket.timeout) as exc:
        raise GXAirflowError("VALIDATION_TIMEOUT", str(exc), True) from exc
    except (OSError, ValueError) as exc:
        raise GXAirflowError("GX_EXECUTION_ERROR", str(exc), True) from exc


def execute_quality_gate(
    payload: dict[str, Any],
    *,
    service_url: str | None = None,
    timeout_seconds: float = 300,
    snapshot_refreshes: int = 1,
    opener: Callable[..., Any] = request.urlopen,
) -> dict[str, Any]:
    endpoint = (service_url or os.getenv("GX_SERVICE_URL", "http://gx-validator:8090")).rstrip(
        "/"
    ) + "/v1/validate/table"
    current_payload = dict(payload)
    for attempt in range(snapshot_refreshes + 1):
        try:
            result = _post_json(endpoint, current_payload, timeout_seconds, opener)
            xcom = {
                key: result[key]
                for key in (
                    "status",
                    "validation_id",
                    "validation_uri",
                    "merge_allowed",
                    "approval",
                )
            }
            if len(json.dumps(xcom).encode("utf-8")) > MAX_XCOM_BYTES:
                raise GXAirflowError(
                    "GX_CONFIGURATION_ERROR",
                    "GX XCom contract exceeds 4096 bytes",
                    False,
                )
            return xcom
        except GXAirflowError as exc:
            current = getattr(exc, "current_identity", None)
            if exc.code == "SNAPSHOT_MISMATCH" and attempt < snapshot_refreshes and current:
                current_payload.update(current)
                continue
            raise
    raise AssertionError("unreachable")


def airflow_quality_gate(**context):
    from airflow.exceptions import AirflowException, AirflowFailException

    payload = context["params"]["validation_request"]
    try:
        return execute_quality_gate(payload)
    except GXAirflowError as exc:
        if exc.code in NON_RETRYABLE or not exc.retryable:
            raise AirflowFailException(str(exc)) from exc
        raise AirflowException(str(exc)) from exc
