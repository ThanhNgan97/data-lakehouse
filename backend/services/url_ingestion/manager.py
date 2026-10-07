import hashlib
import logging
import os
import uuid

import requests
from sqlalchemy.exc import IntegrityError

from core.config import (
    AIRFLOW_FILE_DAG_ID,
    AIRFLOW_WEBSERVER_URL,
    MINIO_BUCKET_NAME,
    URL_CHECKSUM_DEDUP_ENABLED,
)
from db.database import SessionLocal
from db.minio_client import minio_client
from db.models import ImportJob, ImportJobFile, OutboxEvent, ScanCandidate, ScanManifest, SourceFile, UploadHistory

from .contracts import FileCandidate, SourceDescriptor
from .exceptions import UrlIngestionError
from .registry import adapter_for
from .policies import MAX_ACTIVE_FILES_PER_JOB, MAX_JOB_BYTES
from .state import derive_job_status, utcnow


def _candidate(record: ScanCandidate) -> FileCandidate:
    return FileCandidate(
        external_id=record.external_id, name=record.name,
        relative_path=record.relative_path, mime_type=record.mime_type,
        size=record.expected_size, provider_reference=record.provider_reference or "",
        provider_reference_kind=record.provider_reference_kind,
        etag=record.etag, provider_revision=record.provider_revision,
        supported=record.supported, unsupported_reason=record.unsupported_reason,
        metadata=record.metadata_info or {},
    )


def _hash_stream(stream):
    digest = hashlib.sha256() if URL_CHECKSUM_DEDUP_ENABLED else None
    size = 0
    stream.seek(0)
    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        if digest is not None:
            digest.update(chunk)
        size += len(chunk)
    stream.seek(0)
    return digest.hexdigest() if digest is not None else None, size


def _safe_name(value: str) -> str:
    return os.path.basename(value or "download.bin").replace("\\", "_").replace("/", "_")


def _history_for_job_file(db, job_file_id: str):
    return db.query(UploadHistory).filter(
        UploadHistory.metadata_info.contains({"import_job_file_id": job_file_id})
    ).order_by(UploadHistory.id.desc()).first()


def _source_file(db, checksum: str | None, candidate: FileCandidate, size: int, claim_id: str, force_reprocess: bool = False):
    if checksum is None:
        source = SourceFile(
            content_checksum_sha256=None,
            original_filename=_safe_name(candidate.name),
            mime_type=candidate.mime_type,
            file_size_bytes=size,
            processing_status=f"CLAIMED:{claim_id}",
        )
        db.add(source)
        db.commit()
        db.refresh(source)
        return source, "CLAIMED", None
    existing = db.query(SourceFile).filter(SourceFile.content_checksum_sha256 == checksum).first()
    if existing:
        if existing.processing_status == f"CLAIMED:{claim_id}":
            return existing, "CLAIMED", None
        normalized_status = (existing.processing_status or "").upper()
        if normalized_status in {"COMPLETED", "SUCCESS"}:
            if not force_reprocess:
                return existing, "DUPLICATE", None
            existing.processing_status = "FAILED"
            db.commit()
        if normalized_status == "PROCESSING":
            active = db.query(ImportJobFile).filter(
                ImportJobFile.source_file_id == existing.id,
                ImportJobFile.status == "PROCESSING",
                ImportJobFile.dag_run_id.isnot(None),
            ).order_by(ImportJobFile.created_at.desc()).first()
            if active:
                return existing, "PROCESSING", active.dag_run_id
            # No active owner exists: recover a stale processing claim.
            existing.processing_status = "FAILED"
            db.commit()
        if existing.processing_status in {"FAILED", "PENDING"}:
            claimed = db.query(SourceFile).filter(
                SourceFile.id == existing.id,
                SourceFile.processing_status == existing.processing_status,
            ).update({"processing_status": f"CLAIMED:{claim_id}"}, synchronize_session=False)
            db.commit()
            if claimed:
                db.refresh(existing)
                return existing, "CLAIMED", None
        return existing, "PROCESSING", None
    source = SourceFile(
        content_checksum_sha256=checksum, original_filename=_safe_name(candidate.name),
        mime_type=candidate.mime_type, file_size_bytes=size, processing_status=f"CLAIMED:{claim_id}",
    )
    db.add(source)
    try:
        db.commit()
        db.refresh(source)
        return source, "CLAIMED", None
    except IntegrityError:
        db.rollback()
        concurrent = db.query(SourceFile).filter(SourceFile.content_checksum_sha256 == checksum).one()
        return concurrent, "PROCESSING", None


def _trigger_airflow(source: SourceFile, job_file: ImportJobFile):
    retry_suffix = f"__retry_{job_file.retry_count}" if job_file.retry_count else ""
    dag_run_id = f"url_import__{job_file.id}{retry_suffix}"
    response = requests.post(
        f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns",
        json={"dag_run_id": dag_run_id, "conf": {
                "source_file_id": source.id, "import_job_file_id": job_file.id,
                "input_path": source.storage_path, "source_name": source.original_filename,
                "provider": job_file.provider,
            }},
        # Airflow may spend several seconds creating a DAG run while its
        # scheduler is parsing DAGs. Five seconds caused false import failures.
        auth=("airflow", "airflow"), timeout=30,
    )
    if response.status_code == 409:
        return dag_run_id
    response.raise_for_status()
    return response.json().get("dag_run_id") or dag_run_id


def _refresh_job(db, job: ImportJob):
    states = [row[0] for row in db.query(ImportJobFile.status).filter(ImportJobFile.import_job_id == job.id).all()]
    job.status = derive_job_status(states, bool(job.cancel_requested_at))
    if job.status not in {"PENDING", "PROCESSING"}:
        job.completed_at = utcnow()
    db.commit()


def process_import_job(job_id: str):
    """Drain one durable DB-backed job. Safe to call again after a worker restart."""
    db = SessionLocal()
    try:
        job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
        if not job:
            return
        job.status = "PROCESSING"
        job.started_at = job.started_at or utcnow()
        db.commit()
        while True:
            db.refresh(job)
            if job.cancel_requested_at:
                db.query(ImportJobFile).filter(
                    ImportJobFile.import_job_id == job.id, ImportJobFile.status == "QUEUED"
                ).update({"status": "CANCELLED", "completed_at": utcnow()}, synchronize_session=False)
                db.commit()
                break
            active_count = db.query(ImportJobFile).filter(
                ImportJobFile.import_job_id == job.id,
                ImportJobFile.status == "PROCESSING",
            ).count()
            if active_count >= MAX_ACTIVE_FILES_PER_JOB:
                break
            row = db.query(ImportJobFile).filter(
                ImportJobFile.import_job_id == job.id, ImportJobFile.status == "QUEUED"
            ).order_by(ImportJobFile.created_at).first()
            if not row:
                break
            claimed = db.query(ImportJobFile).filter(
                ImportJobFile.id == row.id, ImportJobFile.status == "QUEUED"
            ).update({"status": "DOWNLOADING", "started_at": utcnow(), "worker_id": str(uuid.uuid4())}, synchronize_session=False)
            db.commit()
            if not claimed:
                continue
            row = db.query(ImportJobFile).filter(ImportJobFile.id == row.id).one()
            stream = None
            source = None
            try:
                manifest = db.query(ScanManifest).filter(ScanManifest.id == job.scan_id).one()
                candidate_record = db.query(ScanCandidate).filter(ScanCandidate.id == row.candidate_id).one()
                source_descriptor = SourceDescriptor(
                    manifest.provider, manifest.resource_type, manifest.original_url,
                    manifest.resource_id,
                )
                adapter = adapter_for(manifest.provider)
                candidate = adapter.revalidate(source_descriptor, _candidate(candidate_record))
                download = adapter.download(source_descriptor, candidate)
                stream = download.stream
                checksum, actual_size = _hash_stream(stream)
                row.actual_size = actual_size
                imported_size = sum(value[0] or 0 for value in db.query(ImportJobFile.actual_size).filter(
                    ImportJobFile.import_job_id == job.id,
                    ImportJobFile.id != row.id,
                    ImportJobFile.status.in_(["PROCESSING", "COMPLETED", "DUPLICATE"]),
                ).all())
                if imported_size + actual_size > MAX_JOB_BYTES:
                    raise UrlIngestionError("Tổng dung lượng job vượt giới hạn 1 GB.")
                source, disposition, existing_dag_run_id = _source_file(
                    db, checksum, candidate, actual_size, row.id,
                    force_reprocess=(not URL_CHECKSUM_DEDUP_ENABLED) or row.retry_count > 0,
                )
                row = db.query(ImportJobFile).filter(ImportJobFile.id == row.id).one()
                row.source_file_id = source.id
                if disposition == "DUPLICATE":
                    row.status = "DUPLICATE"
                    row.completed_at = utcnow()
                    history = _history_for_job_file(db, row.id)
                    if history:
                        history.status = "Reused"
                        history.pipeline_status = "success"
                    db.commit()
                    continue
                if disposition == "PROCESSING":
                    row.status = "PROCESSING"
                    row.dag_run_id = existing_dag_run_id
                    history = _history_for_job_file(db, row.id)
                    if history:
                        history.status = "Processing"
                        history.pipeline_status = "running"
                        history.dag_run_id = existing_dag_run_id
                    db.commit()
                    continue
                object_identity = checksum[:12] if checksum else uuid.uuid4().hex[:12]
                object_key = f"staging/{source.id}/{object_identity}/{_safe_name(candidate.name)}"
                minio_client.put_object(MINIO_BUCKET_NAME, object_key, stream, actual_size)
                source.storage_path = object_key
                dag_run_id = _trigger_airflow(source, row)
                source.processing_status = "PROCESSING"
                row.status = "PROCESSING"
                row.dag_run_id = dag_run_id
                history = _history_for_job_file(db, row.id)
                if history:
                    history.file_size_bytes = actual_size
                    history.file_type = candidate.mime_type
                    history.s3_path = object_key
                    history.status = "Uploaded"
                    history.dag_run_id = dag_run_id
                    history.pipeline_status = "running"
                else:
                    db.add(UploadHistory(
                        user_id=job.user_id, filename=candidate.name, file_size_bytes=actual_size,
                        file_type=candidate.mime_type, s3_path=object_key, status="Uploaded",
                        dag_run_id=dag_run_id, pipeline_status="running",
                        metadata_info={"ingestion_method": "URL", "provider": manifest.provider,
                                       "import_job_id": job.id, "import_job_file_id": row.id},
                    ))
                db.commit()
            except Exception as exc:
                db.rollback()
                if source and source.processing_status.startswith("CLAIMED:"):
                    source.processing_status = "FAILED"
                    db.commit()
                failed = db.query(ImportJobFile).filter(ImportJobFile.id == row.id).first()
                if failed:
                    failed.status = "FAILED"
                    failed.error_code = getattr(exc, "code", "IMPORT_FAILED")
                    failed.error_message = str(exc)[:2000]
                    failed.completed_at = utcnow()
                    history = _history_for_job_file(db, failed.id)
                    if history:
                        history.status = "Failed"
                        history.pipeline_status = "failed"
                        metadata = dict(history.metadata_info or {})
                        metadata["error_message"] = failed.error_message
                        history.metadata_info = metadata
                    db.commit()
                logging.exception("URL import file failed: %s", row.id)
            finally:
                if stream is not None:
                    stream.close()
        _refresh_job(db, job)
        event = db.query(OutboxEvent).filter(
            OutboxEvent.aggregate_id == str(job.id), OutboxEvent.event_type == "IMPORT_JOB_CREATED"
        ).first()
        if event:
            event.status = "PUBLISHED"
            event.published_at = utcnow()
            db.commit()
    finally:
        db.close()


def resume_pending_imports():
    """Replay durable outbox events after an API restart."""
    db = SessionLocal()
    try:
        db.query(ImportJobFile).filter(ImportJobFile.status == "DOWNLOADING").update(
            {"status": "QUEUED", "worker_id": None}, synchronize_session=False,
        )
        db.commit()
        job_ids = [row[0] for row in db.query(OutboxEvent.aggregate_id).filter(
            OutboxEvent.event_type == "IMPORT_JOB_CREATED",
            OutboxEvent.status == "PENDING",
        ).order_by(OutboxEvent.created_at).all()]
    finally:
        db.close()
    for job_id in job_ids:
        process_import_job(job_id)
