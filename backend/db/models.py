from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Float, Text
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


class DataConnector(Base):
    __tablename__ = "data_connectors"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    name = Column(String(100), unique=True, index=True, nullable=False)
    connector_type = Column(String(30), default="MYSQL", nullable=False)

    host = Column(String(255), nullable=False)
    port = Column(Integer, default=3306, nullable=False)
    database_name = Column(String(100), nullable=False)
    username = Column(String(100), nullable=False)

    # Không bao giờ trả trực tiếp field này về frontend/API.
    password_encrypted = Column(Text, nullable=False)

    source_config = Column(JSONB, nullable=True)
    schema_mapping = Column(JSONB, nullable=True)

    is_default = Column(Boolean, default=False, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    last_test_status = Column(String(30), default="not_tested", nullable=False)
    last_test_message = Column(String(500), nullable=True)
    last_tested_at = Column(DateTime(timezone=True), nullable=True)

    last_sync_status = Column(String(30), nullable=True)
    last_sync_at = Column(DateTime(timezone=True), nullable=True)

    created_by = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=True,
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    creator = relationship("User", foreign_keys=[created_by])

    def to_dict(self):
        """Safe representation for API responses; excludes password_encrypted."""
        return {
            "id": self.id,
            "name": self.name,
            "connector_type": self.connector_type,
            "host": self.host,
            "port": self.port,
            "database_name": self.database_name,
            "username": self.username,
            "source_config": self.source_config,
            "schema_mapping": self.schema_mapping,
            "is_default": self.is_default,
            "is_active": self.is_active,
            "last_test_status": self.last_test_status,
            "last_test_message": self.last_test_message,
            "last_tested_at": (
                self.last_tested_at.isoformat()
                if self.last_tested_at
                else None
            ),
            "last_sync_status": self.last_sync_status,
            "last_sync_at": (
                self.last_sync_at.isoformat()
                if self.last_sync_at
                else None
            ),
            "created_by": self.created_by,
            "created_at": (
                self.created_at.isoformat()
                if self.created_at
                else None
            ),
            "updated_at": (
                self.updated_at.isoformat()
                if self.updated_at
                else None
            ),
            "has_password": bool(self.password_encrypted),
        }
