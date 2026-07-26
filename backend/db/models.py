from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Float
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
    uploader_username = Column(String(50), ForeignKey("users.username"), index=True, nullable=False)
    filename = Column(String(255), nullable=False)
    file_size_bytes = Column(Float, nullable=False)
    file_type = Column(String(50), nullable=True)
    s3_path = Column(String(500), nullable=False)
    metadata_info = Column(JSONB, nullable=True)  # "metadata" is a reserved word in SQLAlchemy, so we use metadata_info
    status = Column(String(50), default="Uploaded")
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    uploader = relationship("User", foreign_keys=[uploader_username])

    def to_dict(self):
        return {
            "id": self.id,
            "uploader_username": self.uploader_username,
            "filename": self.filename,
            "file_size_bytes": self.file_size_bytes,
            "file_type": self.file_type,
            "s3_path": self.s3_path,
            "metadata_info": self.metadata_info,
            "status": self.status,
            "uploaded_at": self.uploaded_at.isoformat() if self.uploaded_at else None,
        }
