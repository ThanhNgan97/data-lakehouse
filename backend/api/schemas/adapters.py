# -*- coding: utf-8 -*-
"""
api/schemas/adapters.py
------------------------------------------------------------
Pydantic Schemas cho Source Adapters (File, HTTP REST, DB JDBC)
"""

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class HttpAdapterIngestRequest(BaseModel):
    source_name: str = Field(..., description="Tên nguồn dữ liệu REST API (VD: 'LMS Student Portal API')")
    target_dataset: str = Field(..., description="Dataset mục tiêu (VD: 'learning_outcomes')")
    endpoint_url: str = Field(..., description="URL của REST API nguồn")
    method: str = Field("GET", description="Phương thức HTTP (GET, POST)")
    headers: Optional[Dict[str, str]] = Field(default_factory=dict, description="Custom HTTP Headers / Authorization token")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Query Parameters")
    cron_schedule: Optional[str] = Field(None, description="Lịch trình tự động (VD: '0 0 * * *')")


class DbAdapterIngestRequest(BaseModel):
    source_name: str = Field(..., description="Tên hệ thống DB nguồn (VD: 'Oracle Student DB')")
    target_dataset: str = Field(..., description="Dataset mục tiêu (VD: 'teaching_progress')")
    connection_url: str = Field(..., description="Connection string hoặc JDBC URL (VD: 'postgresql://user:pass@host:5432/db')")
    table_or_query: str = Field(..., description="Tên bảng nguồn hoặc câu truy vấn SQL (VD: 'SELECT * FROM courses')")
    incremental_col: Optional[str] = Field(None, description="Cột làm mốc tăng trưởng (VD: 'updated_at')")


class IngestResponse(BaseModel):
    status: str = Field("success", description="Trạng thái xử lý")
    message: str = Field(..., description="Thông báo trả về")
    source_type: str = Field(..., description="Loại nguồn ('file', 'http', 'db')")
    source_name: str = Field(..., description="Tên nguồn")
    checksum: Optional[str] = Field(None, description="Mã băm SHA-256 xác định tính toàn vẹn và trùng lặp")
    s3_path: Optional[str] = Field(None, description="Đường dẫn lưu dữ liệu thô tại MinIO Bronze/Staging")
    records_ingested: Optional[int] = Field(None, description="Số lượng bản ghi nạp được")


class AdapterSourceItem(BaseModel):
    id: str
    name: str
    source_type: str
    target_dataset: str
    status: str
    last_sync: Optional[str] = None


class AdapterListResponse(BaseModel):
    total: int
    adapters: List[AdapterSourceItem]
