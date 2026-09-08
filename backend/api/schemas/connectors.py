from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class ConnectorCreate(BaseModel):
    """Payload dùng khi Admin tạo một Data Connector mới."""

    name: str = Field(..., min_length=1, max_length=100)
    connector_type: str = Field(default="MYSQL", max_length=30)

    host: str = Field(..., min_length=1, max_length=255)
    port: int = Field(default=3306, ge=1, le=65535)

    database_name: str = Field(..., min_length=1, max_length=100)
    username: str = Field(..., min_length=1, max_length=100)

    # Chỉ nhận từ request.
    # Giá trị này sẽ được mã hóa trước khi lưu database.
    password: str = Field(..., min_length=1)

    source_config: Optional[Dict[str, Any]] = None
    schema_mapping: Optional[Dict[str, Any]] = None

    is_active: bool = True


class ConnectorUpdate(BaseModel):
    """Payload dùng khi Admin cập nhật connector."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    host: Optional[str] = Field(default=None, min_length=1, max_length=255)
    port: Optional[int] = Field(default=None, ge=1, le=65535)

    database_name: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
    )

    username: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
    )

    # Nếu không gửi password thì backend phải giữ credential cũ.
    password: Optional[str] = Field(default=None, min_length=1)

    source_config: Optional[Dict[str, Any]] = None
    schema_mapping: Optional[Dict[str, Any]] = None

    is_active: Optional[bool] = None


class ConnectorResponse(BaseModel):
    """Response an toàn trả về frontend."""

    id: int
    name: str
    connector_type: str

    host: str
    port: int
    database_name: str
    username: str

    source_config: Optional[Dict[str, Any]] = None
    schema_mapping: Optional[Dict[str, Any]] = None

    is_default: bool
    is_active: bool

    last_test_status: str
    last_test_message: Optional[str] = None
    last_tested_at: Optional[str] = None

    last_sync_status: Optional[str] = None
    last_sync_at: Optional[str] = None

    created_by: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    has_password: bool


class ConnectorTestResponse(BaseModel):
    success: bool
    message: str

class ConnectorSyncResponse(BaseModel):
    success: bool
    message: str
    connector_id: int
    dag_id: str
    dag_run_id: str | None = None
    state: str | None = None