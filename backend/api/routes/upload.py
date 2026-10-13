import logging
import os
import uuid
import ast
import json
import re
import requests
from threading import Thread
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from pydantic import BaseModel, Field
from api.dependencies import get_current_user
from db.database import get_db
from sqlalchemy.orm import Session
from db.models import ImportJob, ImportJobFile, SourceFile, UploadHistory, User
from db.minio_client import minio_client
from core.config import MAX_UPLOAD_FILE_BYTES, MINIO_BUCKET_NAME
from core.config import (
    AIRFLOW_FILE_DAG_ID,
    AIRFLOW_WEBSERVER_URL,
    SUPERSET_API_URL,
    SUPERSET_ADMIN_PASSWORD,
    SUPERSET_ADMIN_USERNAME,
)
from api.routes.pipeline_preview import _run_trino_query, _validate_table_name
from services.url_ingestion.state import derive_job_status, utcnow
from services.url_ingestion.manager import process_import_job
from services.url_ingestion.policies import SUPPORTED_EXTENSIONS
router = APIRouter()

FILTER_NAMESPACE = uuid.NAMESPACE_DNS
FILTER_COLUMN_PATTERN = re.compile(r"^[a-zA-Z0-9_]{1,128}$")


class ReportFilterStateRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)


def _parse_xcom_value(value):
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        try:
            return ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return None


def _dashboard_result_for_run(dag_run_id: str):
    airflow_base = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns/{dag_run_id}"
    response = requests.get(
        f"{airflow_base}/taskInstances/auto_provision_superset/xcomEntries/return_value",
        auth=("airflow", "airflow"),
        timeout=8,
    )
    if response.status_code != 200:
        return None
    value = _parse_xcom_value(response.json().get("value"))
    return value if isinstance(value, dict) else None


def _dataset_uuid(table_name: str) -> str:
    return str(uuid.uuid5(FILTER_NAMESPACE, f"ctu.lakehouse.dataset.{table_name}"))


def _native_filter_id(table_name: str, column_name: str) -> str:
    dataset_uuid = _dataset_uuid(table_name)
    value = uuid.uuid5(
        FILTER_NAMESPACE,
        f"ctu.lakehouse.filter.select.{dataset_uuid}.{column_name}",
    )
    return f"NATIVE_FILTER-{value}"


def _authorize_dag_run(db: Session, current_user, dag_run_id: str) -> UploadHistory:
    user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
    record = db.query(UploadHistory).filter(
        UploadHistory.dag_run_id == dag_run_id,
        UploadHistory.user_id == user_id,
    ).first()
    if not record:
        raise HTTPException(status_code=404, detail="Không tìm thấy lần upload.")
    return record
@router.get("/")
def read_root():
    return {"message": "Welcome to the Lakehouse API!"}


@router.post("/upload")
@router.post("/upload/")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
       
        # Mọi file người dùng nạp vào đều phải qua tầng Staging.
        safe_filename = os.path.basename(file.filename or "upload.bin")
        extension = os.path.splitext(safe_filename)[1].lower()
        if extension not in SUPPORTED_EXTENSIONS:
            raise HTTPException(
                status_code=415,
                detail="Định dạng chưa được hỗ trợ. Chỉ nhận PDF, DOCX, CSV, TSV, XLSX, XLS, JSON và Parquet.",
            )
        object_name = f"staging/manual/{uuid.uuid4().hex}/{safe_filename}"

        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

        if file_size > MAX_UPLOAD_FILE_BYTES:
            max_size_mb = MAX_UPLOAD_FILE_BYTES // (1024 * 1024)
            raise HTTPException(
                status_code=413,
                detail=f"File vượt giới hạn {max_size_mb} MB.",
            )

   
        if file_size == 0:
            raise HTTPException(status_code=422, detail="File rỗng, không có dữ liệu để xử lý.")

        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=file.file,
            length=file_size
        )

        # Lưu lịch sử upload vào DB
        user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
        
        history_record = UploadHistory(
            user_id=user_id,
            filename=safe_filename,
            file_size_bytes=file_size,
            file_type=file.content_type,
            s3_path=object_name,
            metadata_info={"source": "api", "bucket": MINIO_BUCKET_NAME},
            status="Uploaded"
        )
        db.add(history_record)
        db.commit()

        # Trigger Airflow Pipeline
       

        airflow_url = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns"
        dag_run_id = None
        try:
            # Truyền conf chứa vị trí staging và tên file để AI Semantic Router xử lý
            payload = {
                "conf": {
                    "input_path": object_name,
                    "source_name": safe_filename
                }
            }
            resp = requests.post(airflow_url, json=payload, auth=("airflow", "airflow"), timeout=30)
            if resp.status_code in [200, 201]:
                logging.info("Universal Airflow pipeline triggered successfully.")
                dag_run_id = resp.json().get("dag_run_id")
                history_record.dag_run_id = dag_run_id
                history_record.pipeline_status = "running"
                db.commit()
            else:
                logging.warning(f"Failed to trigger Airflow pipeline [{resp.status_code}]: {resp.text}")
                history_record.pipeline_status = "trigger_failed"
                db.commit()
        except Exception as e:
            logging.error(f"Error triggering Airflow pipeline at {airflow_url}: {e}")
            history_record.pipeline_status = "unreachable"
            db.commit()

        return {
            "message": f"Đã đẩy trực tiếp file {file.filename} vào trạm {object_name} của MinIO và kích hoạt pipeline!",
            "dag_run_id": dag_run_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đẩy file  {str(e)}")

@router.get("/upload/history", summary="Lấy lịch sử tải lên dữ liệu")
async def get_upload_history(
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy danh sách các file đã được upload lên hệ thống.
    Kèm theo thông tin người upload, metadata, kích thước...
    """
    try:
        user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
        histories = (
            db.query(UploadHistory)
            .join(User, UploadHistory.user_id == User.id)
            .filter(UploadHistory.user_id == user_id)
            .order_by(UploadHistory.uploaded_at.desc())
            .limit(limit)
        )
        return [record.to_dict() for record in histories]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi lấy lịch sử: {str(e)}")


@router.get("/upload/report-filters/{dag_run_id}")
def get_report_filters(
    dag_run_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Return data-driven select filters for the dashboard produced by a run."""
    _authorize_dag_run(db, current_user, dag_run_id)
    result = _dashboard_result_for_run(dag_run_id)
    if not result or not result.get("table_name"):
        raise HTTPException(status_code=404, detail="Không tìm thấy dataset của dashboard.")

    table_name = _validate_table_name(result["table_name"])
    try:
        _, rows = _run_trino_query(f"DESCRIBE lakehouse.gold.{table_name}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Không đọc được schema dataset: {exc}")

    priority_terms = (
        "year", "nam", "semester", "hoc_ky", "term", "unit", "don_vi",
        "faculty", "khoa", "department", "phong", "status", "trang_thai",
        "category", "type", "region", "element", "device", "location",
    )
    excluded = {"total_records", "id", "uuid"}
    candidates = []
    for index, row in enumerate(rows):
        column_name = str(row[0]).strip()
        column_type = str(row[1]).lower()
        normalized = column_name.lower()
        if (
            not FILTER_COLUMN_PATTERN.match(column_name)
            or normalized in excluded
            or normalized.startswith("_")
            or normalized.endswith(("_uuid", "_hash"))
        ):
            continue
        is_text = any(value in column_type for value in ("char", "varchar", "boolean"))
        is_named_period = any(value in normalized for value in ("year", "nam", "semester", "hoc_ky", "term"))
        if not is_text and not is_named_period:
            continue
        priority = 0 if any(term in normalized for term in priority_terms) else 1
        candidates.append((priority, index, column_name))

    filters = []
    # The report sidebar is intentionally compact: expose only the first three
    # high-value dimensions selected for this dataset. The dataset selector is
    # rendered separately by the frontend and is not counted in this limit.
    for _, _, column_name in sorted(candidates)[:3]:
        try:
            _, values = _run_trino_query(
                f'SELECT DISTINCT "{column_name}" FROM lakehouse.gold.{table_name} '
                f'WHERE "{column_name}" IS NOT NULL ORDER BY 1 LIMIT 100'
            )
        except Exception:
            continue
        options = [str(row[0]) for row in values if row and row[0] is not None]
        if not options:
            continue
        filters.append({
            "key": column_name,
            "label": column_name.replace("_", " ").strip().title(),
            "native_filter_id": _native_filter_id(table_name, column_name),
            "options": options,
        })

    return {
        "dag_run_id": dag_run_id,
        "dashboard_slug": result.get("dashboard_slug"),
        "table_name": table_name,
        "filters": filters,
    }


@router.post("/upload/report-filter-state/{dag_run_id}")
def create_report_filter_state(
    dag_run_id: str,
    request: ReportFilterStateRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Create a Superset native-filter cache key for external combobox values."""
    _authorize_dag_run(db, current_user, dag_run_id)
    result = _dashboard_result_for_run(dag_run_id)
    if not result or not result.get("dashboard_slug") or not result.get("table_name"):
        raise HTTPException(status_code=404, detail="Không tìm thấy dashboard của pipeline.")
    table_name = _validate_table_name(result["table_name"])

    data_mask = {}
    for column_name, selected_values in request.filters.items():
        if not FILTER_COLUMN_PATTERN.match(column_name):
            raise HTTPException(status_code=400, detail="Tên cột lọc không hợp lệ.")
        values = [str(value) for value in selected_values if str(value)]
        if not values:
            continue
        filter_id = _native_filter_id(table_name, column_name)
        data_mask[filter_id] = {
            "id": filter_id,
            "extraFormData": {"filters": [{"col": column_name, "op": "IN", "val": values}]},
            "filterState": {"value": values},
            "ownState": {},
        }

    superset_url = SUPERSET_API_URL.rstrip("/")
    try:
        login = requests.post(
            f"{superset_url}/api/v1/security/login",
            json={
                "username": SUPERSET_ADMIN_USERNAME,
                "password": SUPERSET_ADMIN_PASSWORD,
                "provider": "db",
                "refresh": True,
            },
            timeout=10,
        )
        login.raise_for_status()
        headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
        slug = result["dashboard_slug"]
        dashboard_response = requests.get(
            f"{superset_url}/api/v1/dashboard/?q=(filters:!((col:slug,opr:eq,value:'{slug}')))" ,
            headers=headers,
            timeout=10,
        )
        dashboard_response.raise_for_status()
        dashboards = dashboard_response.json().get("result", [])
        if not dashboards:
            raise HTTPException(status_code=404, detail="Dashboard không tồn tại trong Superset.")
        dashboard_id = dashboards[0]["id"]
        state_response = requests.post(
            f"{superset_url}/api/v1/dashboard/{dashboard_id}/filter_state",
            headers=headers,
            json={"value": json.dumps(data_mask)},
            timeout=10,
        )
        state_response.raise_for_status()
        return {"key": state_response.json().get("key")}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Không áp dụng được bộ lọc Superset: {exc}")

@router.get("/upload/pipeline-status/{dag_run_id}", summary="Lấy trạng thái step-by-step của Pipeline")
async def get_pipeline_status(
    dag_run_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Gọi Airflow API để lấy trạng thái của DAG run và các task bên trong.
    """
    airflow_base = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns/{dag_run_id}"
    try:
        # Lấy trạng thái tổng quan DAG run (thử universal_lakehouse_pipeline trước, fallback về legacy)
        resp_dag = requests.get(airflow_base, auth=("airflow", "airflow"), timeout=5)
        state = "unknown"
        if resp_dag.status_code == 200:
            state = resp_dag.json().get("state", "unknown")
            
        # Lấy trạng thái các task (taskInstances)
        tasks = []
        resp_tasks = requests.get(f"{airflow_base}/taskInstances", auth=("airflow", "airflow"), timeout=5)
        if resp_tasks.status_code == 200:
            task_instances = resp_tasks.json().get("task_instances", [])
            for t in task_instances:
                tasks.append({
                    "task_id": t.get("task_id"),
                    "state": t.get("state"),
                    "start_date": t.get("start_date"),
                    "end_date": t.get("end_date")
                })

        # Read the metadata returned by the final provisioning task. The
        # frontend owns the browser-facing Superset origin; this API only
        # exposes the stable dashboard identity for the current DAG run.
        dashboard = None
        resp_dashboard = requests.get(
            f"{airflow_base}/taskInstances/auto_provision_superset/xcomEntries/return_value",
            auth=("airflow", "airflow"),
            timeout=5,
        )
        if resp_dashboard.status_code == 200:
            xcom_value = resp_dashboard.json().get("value")
            if isinstance(xcom_value, str):
                try:
                    import json
                    xcom_value = json.loads(xcom_value)
                except (TypeError, ValueError):
                    # Airflow 2.9 returns the non-deserialized XCom value as a
                    # Python mapping repr (single quotes), not valid JSON.
                    try:
                        xcom_value = ast.literal_eval(xcom_value)
                    except (SyntaxError, ValueError):
                        xcom_value = None
            if isinstance(xcom_value, dict) and xcom_value.get("dashboard_slug"):
                dashboard = {
                    "slug": xcom_value["dashboard_slug"],
                    "title": xcom_value.get("dashboard_title"),
                    "imported": bool(xcom_value.get("imported")),
                }
        
        # Đồng bộ trạng thái hiện tại từ Airflow. Một DAG run có thể được clear
        # và retry sau khi Spark đã ghi lỗi vào DB, vì vậy "failed" không được
        # ghim vĩnh viễn nếu chính DAG run đó sau cùng đã thành công.
        record = db.query(UploadHistory).filter(UploadHistory.dag_run_id == dag_run_id).first()

        error_message = None
        parsed_data = None
        if record:
            metadata = dict(record.metadata_info or {})
            if state == "success":
                metadata.pop("error_message", None)
                record.metadata_info = metadata
                record.pipeline_status = "success"
                db.commit()
            elif state == "failed":
                record.pipeline_status = "failed"
                error_message = metadata.get("error_message")
                db.commit()
            elif state != "unknown":
                record.pipeline_status = state
                db.commit()

            if record.metadata_info:
                parsed_data = record.metadata_info.get("parsed_data")

        job_files = db.query(ImportJobFile).filter(ImportJobFile.dag_run_id == dag_run_id).all()
        if job_files and state in {"success", "failed"}:
            transitioned_job_ids = set()
            for job_file in job_files:
                if job_file.status not in {"COMPLETED", "FAILED"}:
                    transitioned_job_ids.add(job_file.import_job_id)
                job_file.status = "COMPLETED" if state == "success" else "FAILED"
                job_file.completed_at = utcnow()
                if state == "failed" and not job_file.error_code:
                    job_file.error_code = "AIRFLOW_FAILED"
                    job_file.error_message = error_message or "Universal pipeline failed."
                if job_file.source_file_id:
                    source_file = db.query(SourceFile).filter(SourceFile.id == job_file.source_file_id).first()
                    if source_file:
                        source_file.processing_status = "COMPLETED" if state == "success" else "FAILED"
                        source_file.processed_at = utcnow()
            for job_id in {item.import_job_id for item in job_files}:
                job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
                if not job:
                    continue
                states = [value[0] for value in db.query(ImportJobFile.status).filter(
                    ImportJobFile.import_job_id == job.id
                ).all()]
                job.status = derive_job_status(states, bool(job.cancel_requested_at))
                if job.status not in {"PENDING", "PROCESSING"}:
                    job.completed_at = utcnow()
            db.commit()
            for job_id in transitioned_job_ids:
                Thread(target=process_import_job, args=(job_id,), daemon=True).start()
            
        return {
            "dag_run_id": dag_run_id,
            "state": state,
            "tasks": tasks,
            "error_message": error_message,
            "parsed_data": parsed_data,
            "dashboard": dashboard,
        }
    except Exception as e:
        return {"state": "unreachable", "error": str(e), "tasks": []}
