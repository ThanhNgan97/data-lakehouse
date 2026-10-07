from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Float, BigInteger, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from db.database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=True)
    full_name = Column(String(100), nullable=True)
    hashed_password = Column(String(255), nullable=False)
    role = Column(String(20), default="user", nullable=False)  # "admin" or "user"
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "full_name": self.full_name,
            "role": self.role,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

class UploadHistory(Base):
    __tablename__ = "upload_history"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    filename = Column(String(255), nullable=False)
    file_size_bytes = Column(Float, nullable=False)
    file_type = Column(String(255), nullable=True)
    s3_path = Column(String(500), nullable=False)
    metadata_info = Column(JSONB, nullable=True)  # "metadata" is a reserved word in SQLAlchemy, so we use metadata_info
    status = Column(String(50), default="Uploaded")
    dag_run_id = Column(String(255), nullable=True)
    pipeline_status = Column(String(50), default="pending", nullable=True)
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    uploader = relationship("User", foreign_keys=[user_id])

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "full_name": self.uploader.full_name if self.uploader else None,
            "username": self.uploader.username if self.uploader else None,
            "filename": self.filename,
            "file_size_bytes": self.file_size_bytes,
            "file_type": self.file_type,
            "s3_path": self.s3_path,
            "metadata_info": self.metadata_info,
            "status": self.status,
            "dag_run_id": self.dag_run_id,
            "pipeline_status": self.pipeline_status,
            "uploaded_at": self.uploaded_at.isoformat() if self.uploaded_at else None,
        }


class ScanManifest(Base):
    __tablename__ = "scan_manifests"

    id = Column(String(36), primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    provider = Column(String(50), nullable=False, index=True)
    resource_type = Column(String(30), nullable=False)
    resource_id = Column(String(512), nullable=True)
    original_url = Column(Text, nullable=False)
    status = Column(String(30), nullable=False, default="ACTIVE", index=True)
    warnings = Column(JSONB, nullable=False, default=list)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)


class ScanCandidate(Base):
    __tablename__ = "scan_candidates"
    __table_args__ = (UniqueConstraint("scan_id", "external_id", name="uq_scan_candidate_external"),)

    id = Column(String(36), primary_key=True)
    scan_id = Column(String(36), ForeignKey("scan_manifests.id", ondelete="CASCADE"), nullable=False, index=True)
    external_id = Column(String(512), nullable=False)
    name = Column(String(512), nullable=False)
    relative_path = Column(Text, nullable=True)
    mime_type = Column(String(255), nullable=True)
    expected_size = Column(BigInteger, nullable=True)
    provider_reference = Column(Text, nullable=True)
    provider_reference_kind = Column(String(50), nullable=False)
    provider_reference_sensitive = Column(Boolean, nullable=False, default=False)
    provider_reference_secret_id = Column(String(512), nullable=True)
    etag = Column(String(512), nullable=True)
    provider_revision = Column(String(512), nullable=True)
    supported = Column(Boolean, nullable=False, default=True)
    unsupported_reason = Column(Text, nullable=True)
    metadata_info = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SourceFile(Base):
    """Content identity. Import provenance belongs to ImportJobFile."""

    __tablename__ = "source_files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Retained as a nullable compatibility column for existing databases.
    # URL/file ingestion no longer calculates or stores SHA-256 values.
    content_checksum_sha256 = Column("checksum_sha256", String(64), unique=True, nullable=True, index=True)
    original_filename = Column(String(512), nullable=False)
    mime_type = Column(String(255), nullable=True)
    file_size_bytes = Column(Float, nullable=False)
    storage_path = Column(Text, nullable=True)
    dataset_type = Column(String(100), nullable=False, default="UNKNOWN")
    classification_confidence = Column(Float, nullable=True)
    processing_status = Column(String(50), nullable=False, default="PENDING", index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    processed_at = Column(DateTime(timezone=True), nullable=True)


class ImportJob(Base):
    __tablename__ = "import_jobs"
    __table_args__ = (UniqueConstraint("user_id", "idempotency_key", name="uq_import_job_idempotency"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    scan_id = Column(String(36), ForeignKey("scan_manifests.id"), nullable=False, index=True)
    input_type = Column(String(30), nullable=False, default="URL")
    # Compatibility with the original import_jobs schema. These columns remain
    # NOT NULL in existing databases, so the URL ingestion flow must populate them.
    source_url = Column(Text, nullable=True)
    recursive = Column(Boolean, nullable=False, default=False)
    discovered_count = Column(Integer, nullable=False, default=0)
    accepted_count = Column(Integer, nullable=False, default=0)
    success_count = Column(Integer, nullable=False, default=0)
    failed_count = Column(Integer, nullable=False, default=0)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    provider = Column(String(50), nullable=False)
    resource_type = Column(String(30), nullable=False)
    status = Column(String(30), nullable=False, default="PENDING", index=True)
    idempotency_key = Column(String(255), nullable=False)
    cancel_requested_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)


class ImportJobFile(Base):
    __tablename__ = "import_job_files"
    __table_args__ = (UniqueConstraint("import_job_id", "candidate_id", name="uq_import_job_candidate"),)

    id = Column(String(36), primary_key=True)
    import_job_id = Column(Integer, ForeignKey("import_jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id = Column(String(36), ForeignKey("scan_candidates.id"), nullable=False)
    source_file_id = Column(Integer, ForeignKey("source_files.id"), nullable=True, index=True)
    status = Column(String(30), nullable=False, default="QUEUED", index=True)
    original_filename = Column(String(512), nullable=False)
    relative_path = Column(Text, nullable=True)
    provider = Column(String(50), nullable=False)
    external_id = Column(String(512), nullable=True)
    source_url = Column(Text, nullable=True)
    original_mime_type = Column(String(255), nullable=True)
    export_mime_type = Column(String(255), nullable=True)
    export_format = Column(String(30), nullable=True)
    expected_size = Column(BigInteger, nullable=True)
    actual_size = Column(BigInteger, nullable=True)
    checksum_sha256 = Column(String(64), nullable=True, index=True)
    dag_run_id = Column(String(255), nullable=True, index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    error_code = Column(String(100), nullable=True)
    error_message = Column(Text, nullable=True)
    worker_id = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(String(36), primary_key=True)
    event_type = Column(String(100), nullable=False, index=True)
    aggregate_id = Column(String(36), nullable=False, index=True)
    payload = Column(JSONB, nullable=False, default=dict)
    status = Column(String(30), nullable=False, default="PENDING", index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    published_at = Column(DateTime(timezone=True), nullable=True)
