import uuid
from typing import Any
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from core.security import decrypt_secret, encrypt_secret
from db.database import get_db
from db.models import ApiSource
from core.config import (
    AIRFLOW_API_DAG_ID,
    AIRFLOW_API_PASSWORD,
    AIRFLOW_API_USERNAME,
    AIRFLOW_WEBSERVER_URL,
    MOCK_API_INTERNAL_URL,
    MOCK_API_KEY,
    MOCK_API_PUBLIC_URL,
)


router = APIRouter()

DEMO_SCENARIOS = {
    "education.learning_outcomes": {
        "name": "Kết quả học tập",
        "path": "/api/v1/education/learning-outcomes",
        "description": "Dữ liệu kết quả học tập theo chương trình, năm học và học kỳ.",
        "dashboard_slug": "ctu-ioc-learning-outcomes",
    },
    "education.teaching_progress": {
        "name": "Tiến độ giảng dạy",
        "path": "/api/v1/education/teaching-progress",
        "description": "Dữ liệu tiến độ giảng dạy theo đơn vị và lớp học phần.",
        "dashboard_slug": "ctu-ioc-teaching-progress",
    },
}


class InspectApiSourceRequest(BaseModel):
    url: str
    bearer_token: str | None = None
    limit: int = 3


class TriggerApiPipelineRequest(BaseModel):
    source_url: str
    bearer_token: str | None = None


class SaveApiSourceRequest(BaseModel):
    name: str
    dataset_id: str
    url: str
    description: str | None = None
    auth_type: str = "none"
    bearer_token: str | None = None
    metadata_info: dict[str, Any] | None = None

def _airflow_auth():
    if not AIRFLOW_API_PASSWORD:
        raise HTTPException(
            status_code=503,
            detail="AIRFLOW_ADMIN_PASSWORD chưa được cấu hình cho backend.",
        )
    return (AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)


def _mock_headers():
    return {"Authorization": f"Bearer {MOCK_API_KEY}"} if MOCK_API_KEY else {}


def _is_configured_mock_url(source_url: str) -> bool:
    source = urlparse(source_url)
    configured = urlparse(MOCK_API_PUBLIC_URL)
    return (
        source.scheme == configured.scheme
        and source.hostname == configured.hostname
        and source.port == configured.port
    )


def _airflow_source_url(source_url: str) -> str:
    parsed = urlparse(source_url)
    if parsed.hostname in {"localhost", "127.0.0.1"} and parsed.port == 8090:
        suffix = parsed.path or "/"
        if parsed.query:
            suffix = f"{suffix}?{parsed.query}"
        return f"{MOCK_API_INTERNAL_URL.rstrip('/')}{suffix}"
    return source_url


def _user_id(current_user):
    user_id = getattr(current_user, "id", None)
    if user_id is None:
        raise HTTPException(status_code=503, detail="Không xác định được người dùng trong cơ sở dữ liệu.")
    return user_id


def _persisted_metadata(request: SaveApiSourceRequest) -> dict[str, Any]:
    metadata = request.metadata_info or {}
    return {
        "dataset_id": request.dataset_id,
        "schema_version": metadata.get("schema_version"),
        "source_system": metadata.get("source_system"),
        "data_as_of": metadata.get("data_as_of"),
        "pagination": metadata.get("pagination") or {},
    }


@router.get("/api-sources")
def list_api_sources(
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return [
        source.to_dict()
        for source in db.query(ApiSource)
        .filter(ApiSource.user_id == _user_id(current_user))
        .order_by(ApiSource.created_at.desc())
        .all()
    ]


@router.post("/api-sources", status_code=status.HTTP_201_CREATED)
def save_api_source(
    request: SaveApiSourceRequest,
    response: Response,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    scenario = DEMO_SCENARIOS.get(request.dataset_id)
    if not scenario:
        raise HTTPException(status_code=422, detail="Dataset chưa được api_dataset_pipeline hỗ trợ.")

    parsed = urlparse(request.url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=422, detail="URL API không hợp lệ.")
    if not request.name.strip():
        raise HTTPException(status_code=422, detail="Tên nguồn dữ liệu không được để trống.")
    if request.auth_type not in {"none", "bearer"}:
        raise HTTPException(status_code=422, detail="Kiểu xác thực không được hỗ trợ.")

    user_id = _user_id(current_user)
    source_url = request.url.strip()
    source = (
        db.query(ApiSource)
        .filter(ApiSource.user_id == user_id, ApiSource.url == source_url)
        .first()
    )
    is_new = source is None
    if is_new:
        source = ApiSource(user_id=user_id, url=source_url)
        db.add(source)

    source.name = request.name.strip()
    source.dataset_id = request.dataset_id
    source.description = (request.description or "").strip() or None
    source.auth_type = request.auth_type
    source.metadata_info = _persisted_metadata(request)
    source.dashboard_slug = scenario["dashboard_slug"]
    source.status = "READY"

    bearer_token = (request.bearer_token or "").strip()
    if not bearer_token and _is_configured_mock_url(source_url) and MOCK_API_KEY:
        bearer_token = MOCK_API_KEY
    if request.auth_type == "none":
        source.credential_ciphertext = None
    elif bearer_token:
        source.credential_ciphertext = encrypt_secret(bearer_token)
    elif is_new:
        raise HTTPException(status_code=422, detail="Bearer Token là bắt buộc cho nguồn xác thực Bearer.")

    try:
        db.commit()
        db.refresh(source)
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Metadata nguồn API xung đột với dữ liệu hiện có.") from exc
    except SQLAlchemyError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Không thể lưu metadata nguồn API vào PostgreSQL.") from exc

    if not is_new:
        response.status_code = status.HTTP_200_OK
    return source.to_dict()


@router.post("/api-sources/inspect")
def inspect_api_source(
    request: InspectApiSourceRequest,
    current_user=Depends(get_current_user),
):
    parsed = urlparse(request.url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise HTTPException(status_code=422, detail="URL API không hợp lệ.")

    headers = {}
    if request.bearer_token:
        headers["Authorization"] = f"Bearer {request.bearer_token}"
    elif _is_configured_mock_url(request.url) and MOCK_API_KEY:
        # The local demo secret stays server-side. Portal users should not
        # need to know infrastructure credentials just to run the demo.
        headers.update(_mock_headers())

    try:
        response = requests.get(
            request.url,
            params={"limit": max(1, min(request.limit, 20))},
            headers=headers,
            timeout=20,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=503, detail=f"Không thể kết nối API nguồn: {exc}") from exc

    if response.status_code != 200:
        if response.status_code == 401:
            raise HTTPException(
                status_code=401,
                detail="API nguồn từ chối xác thực. Hãy kiểm tra Bearer Token hoặc cấu hình MOCK_API_KEY.",
            )
        raise HTTPException(
            status_code=502,
            detail=f"API nguồn trả về HTTP {response.status_code}.",
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="API nguồn không trả về JSON hợp lệ.") from exc

    dataset_id = payload.get("dataset") if isinstance(payload, dict) else None
    scenario = DEMO_SCENARIOS.get(dataset_id)
    if not scenario:
        raise HTTPException(
            status_code=422,
            detail=f"Dataset '{dataset_id or 'không xác định'}' chưa được api_dataset_pipeline hỗ trợ.",
        )

    records = payload.get("data", [])
    if not isinstance(records, list):
        raise HTTPException(status_code=422, detail="Trường data trong phản hồi API phải là một danh sách.")

    return {
        "dataset_id": dataset_id,
        "name": scenario["name"],
        "description": scenario["description"],
        "dashboard_slug": scenario["dashboard_slug"],
        "schema_version": payload.get("schema_version"),
        "source_system": payload.get("source_system"),
        "data_as_of": payload.get("data_as_of"),
        "pagination": payload.get("pagination", {}),
        "data": records[: max(1, min(request.limit, 20))],
    }


@router.get("/api-sources/demo-scenarios")
def list_demo_scenarios(current_user=Depends(get_current_user)):
    return [
        {
            "id": dataset_id,
            "dataset_id": dataset_id,
            "name": scenario["name"],
            "description": scenario["description"],
            "url": f"{MOCK_API_PUBLIC_URL.rstrip('/')}{scenario['path']}",
            "status": "Sẵn sàng demo",
            "dashboard_slug": scenario["dashboard_slug"],
        }
        for dataset_id, scenario in DEMO_SCENARIOS.items()
    ]


@router.get("/api-sources/demo-scenarios/{dataset_id}/preview")
def preview_demo_scenario(
    dataset_id: str,
    limit: int = 3,
    current_user=Depends(get_current_user),
):
    scenario = DEMO_SCENARIOS.get(dataset_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Không tìm thấy kịch bản demo.")

    safe_limit = max(1, min(limit, 20))
    url = f"{MOCK_API_PUBLIC_URL.rstrip('/')}{scenario['path']}"
    try:
        response = requests.get(
            url,
            params={"limit": safe_limit},
            headers=_mock_headers(),
            timeout=15,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Không thể kết nối CTU IOC Mock API: {exc}",
        ) from exc

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Mock API trả về lỗi {response.status_code}: {response.text}",
        )

    payload = response.json()
    return {
        "dataset_id": payload.get("dataset", dataset_id),
        "schema_version": payload.get("schema_version"),
        "source_system": payload.get("source_system"),
        "data_as_of": payload.get("data_as_of"),
        "pagination": payload.get("pagination", {}),
        "data": payload.get("data", []),
    }


@router.post("/api-sources/{dataset_id}/runs", status_code=201)
def trigger_api_dataset_pipeline(
    dataset_id: str,
    request: TriggerApiPipelineRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    parsed = urlparse(request.source_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise HTTPException(status_code=422, detail="URL API nguồn không hợp lệ.")

    source = (
        db.query(ApiSource)
        .filter(
            ApiSource.user_id == _user_id(current_user),
            ApiSource.dataset_id == dataset_id,
            ApiSource.url == request.source_url.strip(),
        )
        .first()
    )
    if not source:
        raise HTTPException(status_code=404, detail="Nguồn API chưa được lưu hoặc không thuộc người dùng hiện tại.")

    bearer_token = (request.bearer_token or "").strip()
    if not bearer_token and source.credential_ciphertext:
        try:
            bearer_token = decrypt_secret(source.credential_ciphertext)
        except ValueError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    if source.auth_type == "bearer" and not bearer_token:
        raise HTTPException(status_code=422, detail="Nguồn API chưa có Bearer Token đã lưu.")

    dag_run_id = f"portal_api__{dataset_id.rsplit('.', 1)[-1]}__{uuid.uuid4().hex[:12]}"
    airflow_url = (
        f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/"
        f"{AIRFLOW_API_DAG_ID}/dagRuns"
    )
    try:
        response = requests.post(
            airflow_url,
            json={
                "dag_run_id": dag_run_id,
                "conf": {
                    "dataset_id": dataset_id,
                    "source_api_url": _airflow_source_url(source.url),
                    "source_api_key": bearer_token,
                },
            },
            auth=_airflow_auth(),
            timeout=30,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Không thể kết nối Airflow: {exc}",
        ) from exc

    if response.status_code not in (200, 201):
        raise HTTPException(
            status_code=502,
            detail=f"Airflow không thể khởi chạy pipeline: {response.text}",
        )

    payload = response.json()
    return {
        "dag_id": AIRFLOW_API_DAG_ID,
        "dag_run_id": payload.get("dag_run_id", dag_run_id),
        "dataset_id": dataset_id,
        "state": payload.get("state", "queued"),
        "dashboard_slug": DEMO_SCENARIOS.get(dataset_id, {}).get("dashboard_slug"),
    }


@router.get("/api-sources/runs/{dag_run_id}")
def get_api_dataset_pipeline_status(
    dag_run_id: str,
    current_user=Depends(get_current_user),
):
    airflow_base = (
        f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/"
        f"{AIRFLOW_API_DAG_ID}/dagRuns/{dag_run_id}"
    )
    try:
        run_response = requests.get(
            airflow_base,
            auth=_airflow_auth(),
            timeout=10,
        )
        if run_response.status_code == 404:
            raise HTTPException(status_code=404, detail="Không tìm thấy lần chạy pipeline.")
        if run_response.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Không đọc được trạng thái Airflow: {run_response.text}",
            )

        tasks_response = requests.get(
            f"{airflow_base}/taskInstances",
            auth=_airflow_auth(),
            timeout=10,
        )
        tasks = []
        if tasks_response.status_code == 200:
            tasks = [
                {
                    "task_id": item.get("task_id"),
                    "state": item.get("state"),
                    "start_date": item.get("start_date"),
                    "end_date": item.get("end_date"),
                }
                for item in tasks_response.json().get("task_instances", [])
            ]

        run = run_response.json()
        return {
            "dag_id": AIRFLOW_API_DAG_ID,
            "dag_run_id": dag_run_id,
            "state": run.get("state", "unknown"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
            "tasks": tasks,
        }
    except HTTPException:
        raise
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Không thể kết nối Airflow: {exc}",
        ) from exc
