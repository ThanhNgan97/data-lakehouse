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
TEACHING_DATA_FILE = APP_DIR / "data" / "teaching_progress.json"

SOURCE_SYSTEM = "ctu_ioc"

# Backward-compatible Learning Outcomes constants used by existing tests/tools.
DATASET = "education.learning_outcomes"
SCHEMA_VERSION = "2.0"

TEACHING_DATASET = "education.teaching_progress"
TEACHING_SCHEMA_VERSION = "1.0-demo"

MAX_LIMIT = 500

app = FastAPI(
    title="CTU IOC Mock API",
    version="1.1.0",
    description=(
        "Mock REST API for Data Lakehouse platform testing. "
        "Learning Outcomes keeps its established mock contract. "
        "Teaching Progress is an explicit DEMO / ASSUMED contract and is NOT "
        "a verified CTU IOC production contract. Endpoints under /mock/* are "
        "test-only."
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
    model_config = ConfigDict(extra="allow")

    record_id: str
    program_code: str
    program_name: str
    academic_year: str = Field(pattern=r"^\d{4}-\d{4}$")
    semester: int = Field(ge=1)

    student_count: int = Field(ge=0)

    passed_course_count: int = Field(ge=0)
    attempted_course_count: int = Field(ge=0)

    gpa_point_sum: float = Field(ge=0)
    gpa_student_count: int = Field(ge=0)

    warning_student_count: int = Field(ge=0)
    dropout_risk_student_count: int = Field(ge=0)

    on_track_student_count: int = Field(ge=0)
    progress_evaluated_student_count: int = Field(ge=0)

    updated_at: datetime
    is_deleted: bool


class TeachingProgressRecord(BaseModel):
    """DEMO / ASSUMED Teaching Progress source record."""

    model_config = ConfigDict(extra="allow")

    record_id: str
    unit_code: str
    unit_name: str
    course_section_code: str
    academic_year: str
    semester: int
    progress_percent: float
    updated_at: datetime


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


class TeachingProgressResponse(BaseModel):
    source_system: str
    dataset: str
    schema_version: str
    generated_at: datetime
    data_as_of: datetime
    data: List[TeachingProgressRecord]
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


def _load_json_records(
    path: Path,
    model: type[BaseModel],
) -> list[dict]:
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload["data"]

    validated = [
        model.model_validate(item)
        for item in records
    ]
    return [
        item.model_dump(mode="json")
        for item in validated
    ]


def _load_baseline() -> list[dict]:
    return _load_json_records(
        DATA_FILE,
        LearningOutcomeRecord,
    )


def _load_teaching_baseline() -> list[dict]:
    return _load_json_records(
        TEACHING_DATA_FILE,
        TeachingProgressRecord,
    )


BASELINE_RECORDS = _load_baseline()
TEACHING_BASELINE_RECORDS = _load_teaching_baseline()

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
        normalized = (
            value[:-1] + "+00:00"
            if value.endswith("Z")
            else value
        )
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
    return (
        base64.urlsafe_b64encode(raw)
        .decode("ascii")
        .rstrip("=")
    )


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0

    try:
        padding = "=" * (-len(cursor) % 4)
        decoded = base64.urlsafe_b64decode(
            (cursor + padding).encode("ascii")
        ).decode("utf-8")
        prefix, value = decoded.split(":", 1)
        if prefix != "offset":
            raise ValueError
        offset = int(value)
        if offset < 0:
            raise ValueError
        return offset
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=400,
            detail="Invalid cursor",
        ) from exc


def _to_aware_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        normalized = (
            value[:-1] + "+00:00"
            if value.endswith("Z")
            else value
        )
        dt = datetime.fromisoformat(normalized)

    if dt.tzinfo is None:
        raise ValueError("updated_at must be timezone-aware")

    return dt


def _sorted_records(records: list[dict]) -> list[dict]:
    return sorted(
        records,
        key=lambda r: (
            _to_aware_datetime(r["updated_at"]),
            r["record_id"],
        ),
    )


def _data_as_of(records: list[dict]) -> datetime:
    if not records:
        return datetime.now(timezone.utc)

    return max(
        _to_aware_datetime(r["updated_at"])
        for r in records
    )


def _page_records(
    records: list[dict],
    *,
    updated_after: Optional[str],
    cursor: Optional[str],
    limit: int,
) -> tuple[list[dict], bool, Optional[str]]:
    watermark = _parse_iso8601(updated_after)
    offset = _decode_cursor(cursor)

    ordered = _sorted_records(
        copy.deepcopy(records)
    )

    if watermark is not None:
        ordered = [
            record
            for record in ordered
            if _to_aware_datetime(record["updated_at"])
            > watermark
        ]

    page = ordered[offset: offset + limit]
    next_offset = offset + len(page)
    has_more = next_offset < len(ordered)

    return (
        page,
        has_more,
        _encode_cursor(next_offset)
        if has_more
        else None,
    )


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
        description=(
            "Return records whose updated_at is strictly newer "
            "than this ISO-8601 timestamp."
        ),
    ),
    cursor: Optional[str] = Query(
        default=None,
        description=(
            "Opaque cursor returned by the previous response."
        ),
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=MAX_LIMIT,
    ),
) -> LearningOutcomesResponse:
    with STATE_LOCK:
        all_records = copy.deepcopy(
            STATE["records"]
        )
        active_scenario = STATE["scenario"]

    page, has_more, next_cursor = _page_records(
        all_records,
        updated_after=updated_after,
        cursor=cursor,
        limit=limit,
    )

    response.headers["X-Mock-API"] = "true"
    response.headers["X-Mock-Scenario"] = active_scenario

    validated_page = [
        LearningOutcomeRecord.model_validate(item)
        for item in page
    ]

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


@app.get(
    "/api/v1/education/teaching-progress",
    response_model=TeachingProgressResponse,
    dependencies=[Depends(require_api_key)],
)
def get_teaching_progress(
    response: Response,
    updated_after: Optional[str] = Query(
        default=None,
        description=(
            "DEMO contract: return records whose updated_at is "
            "strictly newer than this ISO-8601 timestamp."
        ),
    ),
    cursor: Optional[str] = Query(
        default=None,
        description=(
            "Opaque cursor returned by the previous response."
        ),
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=MAX_LIMIT,
    ),
) -> TeachingProgressResponse:
    all_records = copy.deepcopy(
        TEACHING_BASELINE_RECORDS
    )

    page, has_more, next_cursor = _page_records(
        all_records,
        updated_after=updated_after,
        cursor=cursor,
        limit=limit,
    )

    response.headers["X-Mock-API"] = "true"
    response.headers["X-Mock-Scenario"] = (
        "teaching_baseline"
    )
    response.headers["X-Demo-Contract"] = "true"

    validated_page = [
        TeachingProgressRecord.model_validate(item)
        for item in page
    ]

    return TeachingProgressResponse(
        source_system=SOURCE_SYSTEM,
        dataset=TEACHING_DATASET,
        schema_version=TEACHING_SCHEMA_VERSION,
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
        "note": (
            "Test-only Learning Outcomes endpoints. "
            "Do not include /mock/* in the production CTU IOC contract."
        ),
        "scenarios": [
            "baseline",
            "newer_update",
            "soft_delete",
            "new_record",
        ],
    }


@app.post(
    "/mock/scenario/{scenario}",
    response_model=ScenarioResponse,
)
def activate_scenario(
    scenario: str,
) -> ScenarioResponse:
    """
    Reset to the 30-record Learning Outcomes baseline and apply one
    deterministic test mutation.

    This endpoint exists only to test incremental ingestion/update/delete
    behavior. It does not mutate Teaching Progress demo records.
    """
    records = copy.deepcopy(BASELINE_RECORDS)

    if scenario == "baseline":
        note = "30 baseline records restored."

    elif scenario == "newer_update":
        target_id = "DEMO-P001|2025-2026|S2"
        target = next(
            r
            for r in records
            if r["record_id"] == target_id
        )
        target["student_count"] = (
            int(target["student_count"]) + 1
        )
        target["updated_at"] = (
            "2026-09-21T08:00:00+07:00"
        )
        note = (
            f"{target_id}: student_count +1 "
            "with newer updated_at."
        )

    elif scenario == "soft_delete":
        target_id = "DEMO-P001|2025-2026|S2"
        target = next(
            r
            for r in records
            if r["record_id"] == target_id
        )
        target["is_deleted"] = True
        target["updated_at"] = (
            "2026-09-21T08:05:00+07:00"
        )
        note = (
            f"{target_id}: is_deleted=true "
            "with newer updated_at."
        )

    elif scenario == "new_record":
        new_record = {
            "record_id": "DEMO-P011|2025-2026|S2",
            "program_code": "DEMO-P011",
            "program_name": "Chương trình giả lập mới",
            "academic_year": "2025-2026",
            "semester": 2,
            "student_count": 500,
            "passed_course_count": 2100,
            "attempted_course_count": 2500,
            "gpa_point_sum": 1450.0,
            "gpa_student_count": 500,
            "warning_student_count": 18,
            "dropout_risk_student_count": 9,
            "on_track_student_count": 390,
            "progress_evaluated_student_count": 500,
            "updated_at": "2026-09-21T08:10:00+07:00",
            "is_deleted": False,
        }
        LearningOutcomeRecord.model_validate(
            new_record
        )
        records.append(new_record)
        note = (
            "Added deterministic DEMO-P011 record."
        )

    else:
        raise HTTPException(
            status_code=404,
            detail=(
                "Unknown scenario. Use baseline, newer_update, "
                "soft_delete, or new_record."
            ),
        )

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
