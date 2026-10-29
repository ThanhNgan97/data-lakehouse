import logging
import json
import os
import re
import uuid
import ast
import hashlib
import csv
import io
import requests
import time
from threading import Thread
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from api.dependencies import get_current_user
from db.database import get_db
from sqlalchemy.orm import Session
from db.models import ImportJob, ImportJobFile, SourceFile, UploadHistory, User
from db.s3_client import s3_client
from core.config import MAX_UPLOAD_FILE_BYTES, S3_BUCKET_NAME, S3_ENDPOINT, STAGING_PREFIX
from core.config import (
    AIRFLOW_API_PASSWORD,
    AIRFLOW_API_USERNAME,
    AIRFLOW_FILE_DAG_ID,
    AIRFLOW_WEBSERVER_URL,
    OZONE_VALIDATION_DAG_ID,
)
from services.url_ingestion.state import derive_job_status, utcnow
from services.url_ingestion.manager import process_import_job
from services.url_ingestion.policies import SUPPORTED_EXTENSIONS
router = APIRouter()


def _trigger_file_pipeline(record: UploadHistory, db: Session) -> str | None:
    """Create at most one Airflow run per upload, even after an HTTP timeout."""
    run_id = f"file_upload__{(record.metadata_info or {}).get('upload_id', record.id)}"
    base = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}"
    auth = (AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)
    payload = {"dag_run_id": run_id, "conf": {
        "input_path": record.s3_path, "source_name": record.filename,
    }}
    error = "Airflow chưa sẵn sàng."
    for attempt in range(3):
        try:
            # A previous POST may have succeeded even if its response was lost.
            existing = requests.get(f"{base}/dagRuns/{run_id}", auth=auth, timeout=8)
            if existing.status_code == 200:
                record.dag_run_id = run_id
                record.pipeline_status = existing.json().get("state") or "queued"
                break
            ready = requests.get(base, auth=auth, timeout=8)
            if ready.status_code == 200 and ready.json().get("is_active"):
                response = requests.post(f"{base}/dagRuns", json=payload, auth=auth, timeout=15)
                if response.status_code in (200, 201):
                    record.dag_run_id = response.json().get("dag_run_id") or run_id
                    record.pipeline_status = "queued"
                    break
                error = (f"Airflow HTTP {response.status_code}: DAG chưa sẵn sàng."
                         if response.status_code >= 500
                         else f"Airflow HTTP {response.status_code}: {response.text[:200]}")
                if response.status_code not in (409, 500, 502, 503, 504):
                    break
            else:
                error = f"DAG chưa sẵn sàng (HTTP {ready.status_code})."
                if ready.status_code in (401, 403):
                    break
        except requests.RequestException as exc:
            error = f"Không kết nối được Airflow: {type(exc).__name__}"
        if attempt < 2:
            time.sleep(2)
    metadata = dict(record.metadata_info or {})
    if record.dag_run_id:
        metadata.pop("airflow_trigger_error", None)
    else:
        record.pipeline_status = "trigger_failed"
        metadata["airflow_trigger_error"] = error
        logging.warning("Could not trigger file upload %s: %s", record.id, error)
    record.metadata_info = metadata
    db.commit()
    return record.dag_run_id


def _user_value(user, key, default=None):
    if hasattr(user, key):
        return getattr(user, key)
    if isinstance(user, dict):
        return user.get(key, default)
    return default


def _sample_downloaded_object(filename: str, payload: bytes) -> dict:
    """Return a small, non-sensitive readability summary for the UI."""
    extension = os.path.splitext(filename)[1].lower()
    if extension in {".csv", ".tsv"}:
        text = payload.decode("utf-8-sig", errors="replace")
        delimiter = "\t" if extension == ".tsv" else ","
        rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))
        columns = rows[0] if rows else []
        return {
            "format": extension.lstrip("."),
            "readable": bool(rows),
            "column_count": len(columns),
            "sample_row_count": max(0, min(len(rows) - 1, 10)),
        }
    return {
        "format": extension.lstrip(".") or "binary",
        "readable": bool(payload),
        "sample_bytes": len(payload),
    }
def _user_id(user):
    return user.id if hasattr(user, "id") else user.get("id")


def _is_airbyte_pipeline_run(dag_run_id: str) -> bool:
    return re.fullmatch(r"airbyte__\d+", dag_run_id) is not None


def _dashboard_from_xcom(dag_run_id: str):
    airflow_base = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns/{dag_run_id}"
    value = None
    for task_id in ("provision_superset", "auto_provision_superset"):
        response = requests.get(
            f"{airflow_base}/taskInstances/{task_id}/xcomEntries/return_value",
            auth=(AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD),
            timeout=5,
        )
        if response.status_code == 200:
            value = response.json().get("value")
            break
    if value is None:
        return None
    if isinstance(value, str):
        try:
            import json
            value = json.loads(value)
        except (TypeError, ValueError):
            try:
                value = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                value = None
    if not isinstance(value, dict) or not value.get("dashboard_slug"):
        return None
    return {
        "slug": value["dashboard_slug"],
        "title": value.get("dashboard_title"),
        "uuid": value.get("dashboard_uuid"),
        "imported": bool(value.get("imported")),
    }


def _parse_xcom_mapping(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            try:
                value = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                value = None
    return value if isinstance(value, dict) else None


def _gx_from_xcom(dag_run_id: str):
    """Return the bounded GX result for this exact universal DAG run."""
    airflow_base = (
        f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}"
        f"/dagRuns/{dag_run_id}"
    )
    value = None
    for task_id in (
        "gx_validate_file_candidate",
        "onboarding_required",
        "gx_validate_database",
        "gx_validate_file_upload",
    ):
        response = requests.get(
            f"{airflow_base}/taskInstances/{task_id}/xcomEntries/return_value",
            auth=(AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD),
            timeout=5,
        )
        if response.status_code == 200:
            value = _parse_xcom_mapping(response.json().get("value"))
            if value:
                break
    if value is None:
        return None
    allowed = {
        "status", "integration_status", "reason", "dataset_id", "data_asset_ref",
        "source_kind", "validation_id", "validation_uri", "merge_allowed",
        "coverage", "contract_status",
    }
    return {key: value[key] for key in allowed if key in value}


def _gx_task_state(tasks):
    task = next(
        (
            item for item in tasks
            if item.get("task_id") in {
                "gx_validate_file_candidate",
                "onboarding_required",
                "gx_validate_database",
                "gx_validate_file_upload",
            }
            and item.get("state") != "skipped"
        ),
        None,
    )
    if not task:
        return {"status": "NOT_AVAILABLE"}
    state = task.get("state") or "pending"
    return {
        "status": {
            "success": "RESULT_UNAVAILABLE",
            "failed": "FAILED",
            "upstream_failed": "BLOCKED",
            "running": "RUNNING",
        }.get(state, "PENDING"),
    }


def _stored_dashboard(record: UploadHistory):
    if not record.dashboard_slug:
        return None
    return {
        "slug": record.dashboard_slug,
        "title": record.dashboard_title,
        "uuid": record.dashboard_uuid,
        "imported": record.dashboard_status == "ready",
        "status": record.dashboard_status,
    }


def _save_dashboard(record: UploadHistory, dashboard: dict):
    record.dashboard_slug = dashboard["slug"]
    record.dashboard_title = dashboard.get("title")
    record.dashboard_uuid = dashboard.get("uuid")
    record.dashboard_status = "ready" if dashboard.get("imported") else "pending"


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
        upload_id = uuid.uuid4().hex
        object_name = f"{STAGING_PREFIX}/manual/{upload_id}/{safe_filename}"

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

        digest = hashlib.sha256()
        while chunk := file.file.read(1024 * 1024):
            digest.update(chunk)
        source_sha256 = digest.hexdigest()
        file.file.seek(0)

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=object_name,
            Body=file.file,
            ContentLength=file_size,
        )

        # Lưu lịch sử upload vào DB
        user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
        
        history_record = UploadHistory(
            user_id=user_id,
            filename=safe_filename,
            file_size_bytes=file_size,
            file_type=file.content_type,
            s3_path=object_name,
            metadata_info={
                "source": "api",
                "storage": "apache_ozone",
                "bucket": S3_BUCKET_NAME,
                "object_key": object_name,
                "upload_id": upload_id,
                "sha256": source_sha256,
                "archive_status": "PENDING",
            },
            status="Uploaded"
        )
        db.add(history_record)
        db.commit()

        # Trigger Airflow Pipeline
       

        dag_run_id = _trigger_file_pipeline(history_record, db)
        if dag_run_id:
            try:
                airflow_auth = (AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)
                validation_url = (
                    f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/"
                    f"{OZONE_VALIDATION_DAG_ID}/dagRuns"
                )
                validation_payload = {
                    "conf": {
                        "upload_id": upload_id,
                        "bucket": S3_BUCKET_NAME,
                        "object_key": object_name,
                        "filename": safe_filename,
                        "expected_size": file_size,
                        "expected_sha256": source_sha256,
                    }
                }
                validation_resp = requests.post(
                    validation_url,
                    json=validation_payload,
                    auth=airflow_auth,
                    timeout=30,
                )
                metadata = dict(history_record.metadata_info or {})
                if validation_resp.status_code in [200, 201]:
                    metadata["ozone_validation_dag_run_id"] = validation_resp.json().get("dag_run_id")
                    metadata["ozone_validation_status"] = "running"
                else:
                    metadata["ozone_validation_status"] = "trigger_failed"
                    logging.warning(
                        "Failed to trigger Ozone validation DAG [%s]: %s",
                        validation_resp.status_code,
                        validation_resp.text,
                    )
                history_record.metadata_info = metadata
                db.commit()
            except requests.RequestException as exc:
                logging.warning("Ozone validation DAG trigger failed: %s", exc)

        return {
            "message": (
                f"Đã tải file {file.filename} lên Apache Ozone."
                + (" Pipeline đã được kích hoạt." if dag_run_id else " Không thể kích hoạt pipeline.")
            ),
            "upload_id": upload_id,
            "history_id": history_record.id,
            "bucket": S3_BUCKET_NAME,
            "object_key": object_name,
            "sha256": source_sha256,
            "dag_run_id": dag_run_id,
            "airflow_trigger_error": (history_record.metadata_info or {}).get("airflow_trigger_error"),
            "ozone_validation_dag_run_id": (history_record.metadata_info or {}).get(
                "ozone_validation_dag_run_id"
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đẩy file  {str(e)}")


@router.post("/upload/{history_id}/retry-trigger")
def retry_uploaded_file_pipeline(
    history_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    record = db.query(UploadHistory).filter(UploadHistory.id == history_id).with_for_update().first()
    if not record or (record.user_id != _user_value(current_user, "id") and _user_value(current_user, "role") != "admin"):
        raise HTTPException(status_code=404, detail="Không tìm thấy file tải lên.")
    if record.dag_run_id:
        raise HTTPException(status_code=409, detail="File đã có DAG run; không kích hoạt trùng.")
    if record.pipeline_status not in ("trigger_failed", "unreachable"):
        raise HTTPException(status_code=409, detail="File không ở trạng thái cần kích hoạt lại.")
    metadata = record.metadata_info or {}
    try:
        s3_client.head_object(Bucket=metadata.get("bucket") or S3_BUCKET_NAME, Key=record.s3_path)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Không tìm thấy file gốc trong Ozone staging.") from exc
    run_id = _trigger_file_pipeline(record, db)
    return {"dag_run_id": run_id, "pipeline_status": record.pipeline_status,
            "airflow_trigger_error": (record.metadata_info or {}).get("airflow_trigger_error")}


@router.get(
    "/upload/{history_id}/ozone-validation",
    summary="Xác minh file được đọc lại qua Apache Ozone S3 Gateway",
)
async def validate_uploaded_object_in_ozone(
    history_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    record = db.query(UploadHistory).filter(UploadHistory.id == history_id).first()
    current_user_id = _user_value(current_user, "id")
    current_role = _user_value(current_user, "role", "user")
    if not record or (record.user_id != current_user_id and current_role != "admin"):
        raise HTTPException(status_code=404, detail="Không tìm thấy file tải lên.")

    metadata = dict(record.metadata_info or {})
    bucket = metadata.get("bucket") or S3_BUCKET_NAME
    object_key = metadata.get("object_key") or record.s3_path
    expected_sha256 = metadata.get("sha256")

    try:
        head = s3_client.head_object(Bucket=bucket, Key=object_key)
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        body = response["Body"]
        digest = hashlib.sha256()
        sample = bytearray()
        bytes_read = 0
        try:
            for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                digest.update(chunk)
                bytes_read += len(chunk)
                if len(sample) < 64 * 1024:
                    sample.extend(chunk[: (64 * 1024) - len(sample)])
        finally:
            body.close()

        actual_sha256 = digest.hexdigest()
        expected_size = int(record.file_size_bytes)
        actual_size = int(head.get("ContentLength", bytes_read))
        checksum_valid = bool(expected_sha256) and actual_sha256 == expected_sha256
        size_valid = actual_size == expected_size == bytes_read
        sample_result = _sample_downloaded_object(record.filename, bytes(sample))
        passed = checksum_valid and size_valid and sample_result["readable"]

        return {
            "status": "PASS" if passed else "FAIL",
            "message": (
                "Đã đọc file thành công qua Apache Ozone S3 Gateway."
                if passed
                else "File đã được đọc nhưng kết quả xác minh không khớp."
            ),
            "storage": "apache_ozone",
            "endpoint": S3_ENDPOINT,
            "bucket": bucket,
            "object_key": object_key,
            "filename": record.filename,
            "content_length": actual_size,
            "bytes_read": bytes_read,
            "checksum_valid": checksum_valid,
            "size_valid": size_valid,
            "sample": sample_result,
        }
    except Exception as exc:
        logging.exception("Ozone validation failed for upload history %s", history_id)
        raise HTTPException(
            status_code=502,
            detail=f"Không thể đọc file qua Apache Ozone S3 Gateway: {exc}",
        ) from exc

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
        histories = (
            db.query(UploadHistory)
            .join(User, UploadHistory.user_id == User.id)
            .filter(UploadHistory.user_id == _user_id(current_user))
            .order_by(UploadHistory.uploaded_at.desc())
            .limit(limit)
        )
        return [record.to_dict() for record in histories]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi lấy lịch sử: {str(e)}")


@router.get("/upload/history/{history_id}/dashboard", summary="Lấy dashboard của một lần tải lên")
async def get_upload_dashboard(
    history_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    record = db.query(UploadHistory).filter(
        UploadHistory.id == history_id,
        UploadHistory.user_id == _user_id(current_user),
    ).first()
    if not record:
        raise HTTPException(status_code=404, detail="Không tìm thấy lịch sử tải lên.")

    dashboard = _stored_dashboard(record)
    if not dashboard and record.dag_run_id:
        try:
            dashboard = _dashboard_from_xcom(record.dag_run_id)
        except requests.RequestException:
            dashboard = None
        if dashboard:
            _save_dashboard(record, dashboard)
            db.commit()
            dashboard = _stored_dashboard(record)

    return {
        "history_id": record.id,
        "dag_run_id": record.dag_run_id,
        "pipeline_status": record.pipeline_status,
        "dashboard": dashboard,
    }

@router.get("/upload/pipeline-status/{dag_run_id}", summary="Lấy trạng thái step-by-step của Pipeline")
async def get_pipeline_status(
    dag_run_id: str,
    context_id: str | None = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Gọi Airflow API để lấy trạng thái của DAG run và các task bên trong.
    """
    record = db.query(UploadHistory).filter(
        UploadHistory.dag_run_id == dag_run_id,
        UploadHistory.user_id == _user_id(current_user),
    ).first()
    is_airbyte_run = _is_airbyte_pipeline_run(dag_run_id)
    if not record and not is_airbyte_run:
        raise HTTPException(status_code=404, detail="Không tìm thấy pipeline của người dùng.")
    if not record and not (context_id or "").strip():
        raise HTTPException(
            status_code=422,
            detail="Pipeline database cần context_id để xác minh nguồn dữ liệu.",
        )

    airflow_base = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns/{dag_run_id}"
    try:
        # Lấy trạng thái tổng quan DAG run (thử universal_lakehouse_pipeline trước, fallback về legacy)
        airflow_auth = (AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)
        resp_dag = requests.get(airflow_base, auth=airflow_auth, timeout=5)
        state = "unknown"
        if resp_dag.status_code == 200:
            dag_payload = resp_dag.json()
            if not record:
                dag_context_id = str((dag_payload.get("conf") or {}).get("context_id") or "").strip()
                if not dag_context_id or dag_context_id != context_id.strip():
                    raise HTTPException(status_code=404, detail="Không tìm thấy pipeline database phù hợp.")
            state = dag_payload.get("state", "unknown")
        elif not record:
            raise HTTPException(status_code=404, detail="Không tìm thấy pipeline database trên Airflow.")
            
        # Lấy trạng thái các task (taskInstances)
        tasks = []
        resp_tasks = requests.get(f"{airflow_base}/taskInstances", auth=airflow_auth, timeout=5)
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
        dashboard = _stored_dashboard(record) if record else None
        try:
            xcom_dashboard = _dashboard_from_xcom(dag_run_id)
        except requests.RequestException:
            xcom_dashboard = None
        if xcom_dashboard:
            if record:
                _save_dashboard(record, xcom_dashboard)
                dashboard = _stored_dashboard(record)
            else:
                dashboard = xcom_dashboard

        try:
            gx_validation = _gx_from_xcom(dag_run_id)
        except requests.RequestException:
            gx_validation = None
        if gx_validation is None:
            gx_validation = _gx_task_state(tasks)
        
        # Đồng bộ trạng thái hiện tại từ Airflow. Một DAG run có thể được clear
        # và retry sau khi Spark đã ghi lỗi vào DB, vì vậy "failed" không được
        # ghim vĩnh viễn nếu chính DAG run đó sau cùng đã thành công.
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
            "gx_validation": gx_validation,
        }
    except HTTPException:
        raise
    except Exception as e:
        return {"state": "unreachable", "error": str(e), "tasks": []}
