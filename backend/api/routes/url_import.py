import uuid
from datetime import timedelta

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel, HttpUrl
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from core.config import GOOGLE_DRIVE_INGESTION_ENABLED, URL_INGESTION_ENABLED
from db.database import get_db
from db.models import ImportJob, ImportJobFile, OutboxEvent, ScanCandidate, ScanManifest, SourceFile, UploadHistory
from services.url_ingestion.exceptions import UrlIngestionError
from services.url_ingestion.manager import process_import_job
from services.url_ingestion.policies import MAX_FILES_PER_JOB, MAX_JOB_BYTES, SCAN_TTL_MINUTES
from services.url_ingestion.registry import adapter_for
from services.url_ingestion.resolver import resolve_url
from services.url_ingestion.state import derive_job_status, utcnow

router = APIRouter(prefix="/url-import")


class ScanRequest(BaseModel):
    url: HttpUrl
    recursive: bool = False


class CreateJobRequest(BaseModel):
    scan_id: str
    candidate_ids: list[str]
    force_reprocess: bool = False


def _user_id(user):
    return user.id if hasattr(user, "id") else user.get("id")


def _scan_payload(db: Session, manifest: ScanManifest):
    now = utcnow()
    if manifest.status == "ACTIVE" and manifest.expires_at <= now:
        manifest.status = "EXPIRED"
        db.commit()
    candidates = db.query(ScanCandidate).filter(ScanCandidate.scan_id == manifest.id).all()
    return {
        "scan_id": manifest.id, "provider": manifest.provider,
        "resource_type": manifest.resource_type, "status": manifest.status,
        "expires_at": manifest.expires_at.isoformat(), "warnings": manifest.warnings or [],
        "summary": {
            "discovered": len(candidates),
            "accepted": sum(1 for item in candidates if item.supported),
            "unsupported": sum(1 for item in candidates if not item.supported),
        },
        "files": [{
            "candidate_id": item.id, "name": item.name,
            "relative_path": item.relative_path, "mime_type": item.mime_type,
            "size": item.expected_size, "supported": item.supported,
            "reason": item.unsupported_reason,
        } for item in candidates],
    }


@router.post("/scan")
def scan_url(payload: ScanRequest, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    try:
        if not URL_INGESTION_ENABLED:
            raise HTTPException(status_code=503, detail="URL Ingestion đang tắt.")
        source = resolve_url(str(payload.url))
        if source.provider == "GOOGLE_DRIVE" and not GOOGLE_DRIVE_INGESTION_ENABLED:
            raise HTTPException(status_code=503, detail="Nguồn Google Drive tạm thời không khả dụng. Vui lòng liên hệ quản trị viên.")
        if payload.recursive:
            raise HTTPException(status_code=422, detail="MVP chưa hỗ trợ quét folder đệ quy.")
        result = adapter_for(source.provider).scan(source)
        manifest = ScanManifest(
            id=str(uuid.uuid4()), user_id=_user_id(current_user), provider=source.provider,
            resource_type=source.resource_type, resource_id=source.resource_id,
            original_url=source.original_url, status="ACTIVE", warnings=result.warnings,
            expires_at=utcnow() + timedelta(minutes=SCAN_TTL_MINUTES),
        )
        db.add(manifest)
        db.flush()  # Parent must exist before bulk candidate inserts.
        for candidate in result.files:
            db.add(ScanCandidate(
                id=str(uuid.uuid4()), scan_id=manifest.id, external_id=candidate.external_id,
                name=candidate.name, relative_path=candidate.relative_path,
                mime_type=candidate.mime_type, expected_size=candidate.size,
                provider_reference=candidate.provider_reference,
                provider_reference_kind=candidate.provider_reference_kind,
                provider_reference_sensitive=False, etag=candidate.etag,
                provider_revision=candidate.provider_revision, supported=candidate.supported,
                unsupported_reason=candidate.unsupported_reason, metadata_info=candidate.metadata,
            ))
        db.commit()
        return _scan_payload(db, manifest)
    except HTTPException:
        raise
    except UrlIngestionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/scans/{scan_id}")
def get_scan(scan_id: str, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    manifest = db.query(ScanManifest).filter(
        ScanManifest.id == scan_id, ScanManifest.user_id == _user_id(current_user)
    ).first()
    if not manifest:
        raise HTTPException(status_code=404, detail="Không tìm thấy manifest.")
    return _scan_payload(db, manifest)


@router.post("/jobs")
def create_job(
    payload: CreateJobRequest, background_tasks: BackgroundTasks,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: Session = Depends(get_db), current_user=Depends(get_current_user),
):
    user_id = _user_id(current_user)
    existing = db.query(ImportJob).filter(
        ImportJob.user_id == user_id, ImportJob.idempotency_key == idempotency_key
    ).first()
    if existing:
        return _job_payload(db, existing)
    manifest = db.query(ScanManifest).filter(
        ScanManifest.id == payload.scan_id, ScanManifest.user_id == user_id
    ).first()
    if not manifest:
        raise HTTPException(status_code=404, detail="Không tìm thấy manifest.")
    if manifest.expires_at <= utcnow() or manifest.status != "ACTIVE":
        manifest.status = "EXPIRED"
        db.commit()
        raise HTTPException(status_code=410, detail="Manifest đã hết hạn; vui lòng Scan lại.")
    selected_ids = list(dict.fromkeys(payload.candidate_ids))
    if not selected_ids or len(selected_ids) > MAX_FILES_PER_JOB:
        raise HTTPException(status_code=422, detail=f"Chọn từ 1 đến {MAX_FILES_PER_JOB} file.")
    candidates = db.query(ScanCandidate).filter(
        ScanCandidate.scan_id == manifest.id, ScanCandidate.id.in_(selected_ids)
    ).all()
    if len(candidates) != len(selected_ids):
        raise HTTPException(status_code=422, detail="Candidate không thuộc manifest.")
    if any(not item.supported for item in candidates):
        raise HTTPException(status_code=422, detail="Danh sách có file không được hỗ trợ.")
    known_total = sum(item.expected_size or 0 for item in candidates)
    if known_total > MAX_JOB_BYTES:
        raise HTTPException(status_code=413, detail="Tổng dung lượng job vượt giới hạn 1 GB.")
    discovered_count = db.query(ScanCandidate).filter(ScanCandidate.scan_id == manifest.id).count()
    job = ImportJob(
        user_id=user_id, scan_id=manifest.id,
        input_type="URL", source_url=manifest.original_url, recursive=False,
        discovered_count=discovered_count, accepted_count=len(candidates),
        success_count=0, failed_count=0, created_by=user_id,
        provider=manifest.provider, resource_type=manifest.resource_type,
        status="PENDING", idempotency_key=idempotency_key,
    )
    db.add(job)
    db.flush()
    for candidate in candidates:
        metadata = candidate.metadata_info or {}
        job_file_id = str(uuid.uuid4())
        db.add(ImportJobFile(
            id=job_file_id, import_job_id=job.id, candidate_id=candidate.id,
            status="QUEUED", original_filename=candidate.name,
            relative_path=candidate.relative_path, provider=manifest.provider,
            external_id=candidate.external_id, source_url=manifest.original_url,
            original_mime_type=metadata.get("original_mime_type", candidate.mime_type),
            export_mime_type=metadata.get("export_mime_type"),
            export_format=metadata.get("export_format"), expected_size=candidate.expected_size,
            retry_count=1 if payload.force_reprocess else 0,
        ))
        # Show every selected URL file in upload history immediately.  This
        # record is updated in-place as the serial worker downloads and runs it.
        db.add(UploadHistory(
            user_id=user_id,
            filename=candidate.name,
            file_size_bytes=candidate.expected_size or 0,
            file_type=candidate.mime_type,
            s3_path=f"pending://url-import/{job_file_id}",
            status="Queued",
            pipeline_status="queued",
            metadata_info={
                "ingestion_method": "URL",
                "provider": manifest.provider,
                "import_job_id": job.id,
                "import_job_file_id": job_file_id,
                "source_url": manifest.original_url,
            },
        ))
    db.add(OutboxEvent(
        id=str(uuid.uuid4()), event_type="IMPORT_JOB_CREATED",
        aggregate_id=str(job.id), payload={"job_id": job.id}, status="PENDING",
    ))
    manifest.status = "CONSUMED"
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        job = db.query(ImportJob).filter(
            ImportJob.user_id == user_id, ImportJob.idempotency_key == idempotency_key
        ).one()
    background_tasks.add_task(process_import_job, job.id)
    return _job_payload(db, job)


def _job_payload(db: Session, job: ImportJob):
    files = db.query(ImportJobFile).filter(ImportJobFile.import_job_id == job.id).order_by(ImportJobFile.created_at).all()
    states = [item.status for item in files]
    derived = derive_job_status(states, bool(job.cancel_requested_at))
    counts = {state: states.count(state) for state in sorted(set(states))}
    return {
        "job_id": job.id, "scan_id": job.scan_id, "provider": job.provider,
        "resource_type": job.resource_type, "status": derived,
        "counts": counts, "created_at": job.created_at.isoformat() if job.created_at else None,
        "files": [{
            "id": item.id, "candidate_id": item.candidate_id,
            "name": item.original_filename, "relative_path": item.relative_path,
            "status": item.status, "source_file_id": item.source_file_id,
            "dag_run_id": item.dag_run_id, "actual_size": item.actual_size,
            "error_code": item.error_code, "error_message": item.error_message,
        } for item in files],
    }


@router.get("/jobs/{job_id}")
def get_job(job_id: str, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    job = db.query(ImportJob).filter(
        ImportJob.id == job_id, ImportJob.user_id == _user_id(current_user)
    ).first()
    if not job:
        raise HTTPException(status_code=404, detail="Không tìm thấy import job.")
    return _job_payload(db, job)


@router.post("/jobs/{job_id}/retry")
def retry_job(job_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    job = db.query(ImportJob).filter(ImportJob.id == job_id, ImportJob.user_id == _user_id(current_user)).first()
    if not job:
        raise HTTPException(status_code=404, detail="Không tìm thấy import job.")
    db.query(ImportJobFile).filter(
        ImportJobFile.import_job_id == job.id, ImportJobFile.status == "FAILED"
    ).update({"status": "QUEUED", "error_code": None, "error_message": None,
              "retry_count": ImportJobFile.retry_count + 1}, synchronize_session=False)
    # A file may have been labelled DUPLICATE while another job with the same
    # checksum was still running. If that shared source later failed, it must
    # be eligible for a real retry instead of remaining a false success.
    duplicate_files = db.query(ImportJobFile).filter(
        ImportJobFile.import_job_id == job.id,
        ImportJobFile.status == "DUPLICATE",
        ImportJobFile.source_file_id.isnot(None),
    ).all()
    for item in duplicate_files:
        source = db.query(SourceFile).filter(SourceFile.id == item.source_file_id).first()
        if source and (source.processing_status or "").upper() == "FAILED":
            item.status = "QUEUED"
            item.error_code = None
            item.error_message = None
            item.retry_count += 1
    job.status = "PROCESSING"
    db.commit()
    background_tasks.add_task(process_import_job, job.id)
    return _job_payload(db, job)


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    job = db.query(ImportJob).filter(ImportJob.id == job_id, ImportJob.user_id == _user_id(current_user)).first()
    if not job:
        raise HTTPException(status_code=404, detail="Không tìm thấy import job.")
    job.cancel_requested_at = utcnow()
    db.commit()
    return _job_payload(db, job)
