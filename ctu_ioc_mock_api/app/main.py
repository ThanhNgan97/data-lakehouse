from __future__ import annotations

import base64
import copy
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field


APP_DIR = Path(__file__).resolve().parent
DATA_FILE = APP_DIR / "data" / "learning_outcomes.json"

SOURCE_SYSTEM = "ctu_ioc"
DATASET = "education.learning_outcomes"
SCHEMA_VERSION = "1.0"
MAX_LIMIT = 500

app = FastAPI(
    title="CTU IOC Mock API",
    version="1.0.0",
    description=(
        "Mock REST API for testing CTU IOC Learning Outcomes ingestion into the "
        "Data Lakehouse. Endpoints under /mock/* are test-only and are NOT part "
        "of the proposed production CTU IOC contract."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class LearningOutcomeRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ma_ban_ghi: str
    ma_chuong_trinh: str
    ten_chuong_trinh: str
    nam_hoc: str = Field(pattern=r"^\d{4}-\d{4}$")
    hoc_ky: int = Field(ge=1)

    so_sinh_vien: int = Field(ge=0)

    so_luot_hoc_phan_dat: int = Field(ge=0)
    tong_luot_hoc_phan: int = Field(ge=0)

    tong_diem_gpa: float = Field(ge=0)
    so_sinh_vien_tinh_gpa: int = Field(ge=0)

    so_sinh_vien_canh_bao: int = Field(ge=0)
    so_sinh_vien_nguy_co_nghi_hoc: int = Field(ge=0)

    so_sinh_vien_dung_tien_do: int = Field(ge=0)
    so_sinh_vien_danh_gia_tien_do: int = Field(ge=0)

    thoi_gian_cap_nhat_nguon: datetime
    da_xoa: bool


class Pagination(BaseModel):
    returned_records: int
    has_more: bool
    next_cursor: Optional[str] = None


class LearningOutcomesResponse(BaseModel):
    source_system: str
    dataset: str
    schema_version: str
    generated_at: datetime
    data_as_of: datetime
    data: List[LearningOutcomeRecord]
    pagination: Pagination


class HealthResponse(BaseModel):
    status: str
    service: str
    dataset: str
    schema_version: str
    records_loaded: int
    active_scenario: str


class ScenarioResponse(BaseModel):
    active_scenario: str
    records_loaded: int
    note: str


def _load_baseline() -> list[dict]:
    import json

    payload = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    records = payload["data"]

    # Validate every static mock record against the contract at startup.
    validated = [LearningOutcomeRecord.model_validate(item) for item in records]
    return [item.model_dump(mode="json") for item in validated]


BASELINE_RECORDS = _load_baseline()
STATE_LOCK = Lock()
STATE = {
    "scenario": "baseline",
    "records": copy.deepcopy(BASELINE_RECORDS),
}


def require_api_key(
    authorization: Optional[str] = Header(default=None),
) -> None:
    """
    Authentication is optional for local testing.

    If MOCK_API_KEY is not set, the endpoint is open.
    If MOCK_API_KEY is set, clients must send:
        Authorization: Bearer <MOCK_API_KEY>
    """
    expected = os.getenv("MOCK_API_KEY")
    if not expected:
        return

    if authorization != f"Bearer {expected}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid bearer token",
        )


def _parse_iso8601(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None

    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="updated_after must be a valid ISO-8601 timestamp",
        ) from exc

    if parsed.tzinfo is None:
        raise HTTPException(
            status_code=400,
            detail="updated_after must include a timezone",
        )

    return parsed


def _encode_cursor(offset: int) -> str:
    raw = f"offset:{offset}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0

    try:
        padding = "=" * (-len(cursor) % 4)
        decoded = base64.urlsafe_b64decode((cursor + padding).encode("ascii")).decode("utf-8")
        prefix, value = decoded.split(":", 1)
        if prefix != "offset":
            raise ValueError
        offset = int(value)
        if offset < 0:
            raise ValueError
        return offset
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc


def _to_aware_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError("thoi_gian_cap_nhat_nguon must be timezone-aware")
    return dt


def _sorted_records(records: list[dict]) -> list[dict]:
    return sorted(
        records,
        key=lambda r: (_to_aware_datetime(r["thoi_gian_cap_nhat_nguon"]), r["ma_ban_ghi"]),
    )


def _data_as_of(records: list[dict]) -> datetime:
    if not records:
        return datetime.now(timezone.utc)

    return max(_to_aware_datetime(r["thoi_gian_cap_nhat_nguon"]) for r in records)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    with STATE_LOCK:
        return HealthResponse(
            status="ok",
            service="ctu-ioc-mock-api",
            dataset=DATASET,
            schema_version=SCHEMA_VERSION,
            records_loaded=len(STATE["records"]),
            active_scenario=STATE["scenario"],
        )


@app.get(
    "/api/v1/education/learning-outcomes",
    response_model=LearningOutcomesResponse,
    dependencies=[Depends(require_api_key)],
)
def get_learning_outcomes(
    response: Response,
    updated_after: Optional[str] = Query(
        default=None,
        description="Return records whose thoi_gian_cap_nhat_nguon is strictly newer than this ISO-8601 timestamp.",
    ),
    cursor: Optional[str] = Query(
        default=None,
        description="Opaque cursor returned by the previous response.",
    ),
    limit: int = Query(default=100, ge=1, le=MAX_LIMIT),
) -> LearningOutcomesResponse:
    watermark = _parse_iso8601(updated_after)
    offset = _decode_cursor(cursor)

    with STATE_LOCK:
        all_records = copy.deepcopy(STATE["records"])
        active_scenario = STATE["scenario"]

    records = _sorted_records(all_records)

    if watermark is not None:
        records = [
            r for r in records
            if _to_aware_datetime(r["thoi_gian_cap_nhat_nguon"]) > watermark
        ]

    page = records[offset : offset + limit]
    next_offset = offset + len(page)
    has_more = next_offset < len(records)
    next_cursor = _encode_cursor(next_offset) if has_more else None

    response.headers["X-Mock-API"] = "true"
    response.headers["X-Mock-Scenario"] = active_scenario

    validated_page = [LearningOutcomeRecord.model_validate(item) for item in page]

    return LearningOutcomesResponse(
        source_system=SOURCE_SYSTEM,
        dataset=DATASET,
        schema_version=SCHEMA_VERSION,
        generated_at=datetime.now(timezone.utc),
        data_as_of=_data_as_of(all_records),
        data=validated_page,
        pagination=Pagination(
            returned_records=len(validated_page),
            has_more=has_more,
            next_cursor=next_cursor,
        ),
    )


@app.get("/mock/scenarios")
def list_scenarios():
    return {
        "note": "Test-only endpoints. Do not include /mock/* in the production CTU IOC contract.",
        "scenarios": [
            "baseline",
            "newer_update",
            "soft_delete",
            "new_record",
        ],
    }


@app.post("/mock/scenario/{scenario}", response_model=ScenarioResponse)
def activate_scenario(scenario: str) -> ScenarioResponse:
    """
    Reset to the 30-record baseline and apply one deterministic test mutation.

    This endpoint exists only to test incremental ingestion/update/delete behavior.
    It is intentionally outside /api/v1/... so it cannot be confused with the
    proposed CTU IOC production API.
    """
    records = copy.deepcopy(BASELINE_RECORDS)

    if scenario == "baseline":
        note = "30 baseline records restored."

    elif scenario == "newer_update":
        target_id = "DEMO-P001|2025-2026|S2"
        target = next(r for r in records if r["ma_ban_ghi"] == target_id)
        target["so_sinh_vien"] = int(target["so_sinh_vien"]) + 1
        target["thoi_gian_cap_nhat_nguon"] = "2026-09-21T08:00:00+07:00"
        note = f"{target_id}: so_sinh_vien +1 with newer thoi_gian_cap_nhat_nguon."

    elif scenario == "soft_delete":
        target_id = "DEMO-P001|2025-2026|S2"
        target = next(r for r in records if r["ma_ban_ghi"] == target_id)
        target["da_xoa"] = True
        target["thoi_gian_cap_nhat_nguon"] = "2026-09-21T08:05:00+07:00"
        note = f"{target_id}: da_xoa=true with newer thoi_gian_cap_nhat_nguon."

    elif scenario == "new_record":
        new_record = {
            "ma_ban_ghi": "DEMO-P011|2025-2026|S2",
            "ma_chuong_trinh": "DEMO-P011",
            "ten_chuong_trinh": "Chương trình giả lập mới",
            "nam_hoc": "2025-2026",
            "hoc_ky": 2,
            "so_sinh_vien": 500,
            "so_luot_hoc_phan_dat": 2100,
            "tong_luot_hoc_phan": 2500,
            "tong_diem_gpa": 1450.0,
            "so_sinh_vien_tinh_gpa": 500,
            "so_sinh_vien_canh_bao": 18,
            "so_sinh_vien_nguy_co_nghi_hoc": 9,
            "so_sinh_vien_dung_tien_do": 390,
            "so_sinh_vien_danh_gia_tien_do": 500,
            "thoi_gian_cap_nhat_nguon": "2026-09-21T08:10:00+07:00",
            "da_xoa": False,
        }
        LearningOutcomeRecord.model_validate(new_record)
        records.append(new_record)
        note = "Added deterministic DEMO-P011 record."

    else:
        raise HTTPException(
            status_code=404,
            detail="Unknown scenario. Use baseline, newer_update, soft_delete, or new_record.",
        )

    # Validate the final state before publishing it.
    for item in records:
        LearningOutcomeRecord.model_validate(item)

    with STATE_LOCK:
        STATE["scenario"] = scenario
        STATE["records"] = records

    return ScenarioResponse(
        active_scenario=scenario,
        records_loaded=len(records),
        note=note,
    )
