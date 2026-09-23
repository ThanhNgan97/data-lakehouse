# -*- coding: utf-8 -*-
"""
api/schemas/registry.py
------------------------------------------------------------
Pydantic Schemas cho Dataset Registry (Schema, Mapping, Key, DQ Rules, Version)
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class ColumnMapping(BaseModel):
    source_column: str = Field(..., description="Tên cột dữ liệu nguồn (Bronze/Raw)")
    target_column: str = Field(..., description="Tên cột dữ liệu đích (Silver)")
    data_type: str = Field("string", description="Kiểu dữ liệu (string, integer, float, timestamp, boolean)")
    is_primary_key: bool = Field(False, description="Đánh dấu nếu cột này thuộc Khóa chính / Deduplication Key")
    is_nullable: bool = Field(True, description="Cho phép giá trị NULL hay không")
    description: Optional[str] = Field(None, description="Mô tả cột")


class DataQualityRule(BaseModel):
    rule_id: str = Field(..., description="Mã định danh luật (VD: 'DQ_001')")
    rule_name: str = Field(..., description="Tên quy tắc (VD: 'Check Score Range')")
    rule_type: str = Field(..., description="Loại quy tắc: 'NOT_NULL', 'VALUE_RANGE', 'REGEX_MATCH', 'CUSTOM_SQL'")
    column_name: str = Field(..., description="Tên cột áp dụng quy tắc")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Tham số luật (VD: {'min': 0, 'max': 10})")
    severity: str = Field("ERROR", description="Mức độ nghiêm trọng: 'WARNING', 'ERROR'")


class DatasetCreate(BaseModel):
    dataset_name: str = Field(..., description="Tên định danh Dataset (VD: 'learning_outcomes')")
    display_name: str = Field(..., description="Tên hiển thị tiếng Việt (VD: 'Kết quả Học tập')")
    domain: str = Field(..., description="Miền dữ liệu ('learning', 'teaching', 'kpi')")
    description: Optional[str] = Field(None, description="Mô tả chi tiết Dataset")
    primary_keys: List[str] = Field(default_factory=list, description="Danh sách các cột khóa chính dùng cho MERGE")
    mappings: List[ColumnMapping] = Field(default_factory=list, description="Danh sách quy tắc Mapping cột")
    dq_rules: List[DataQualityRule] = Field(default_factory=list, description="Danh sách các luật kiểm tra chất lượng DQ")


class SchemaVersionInfo(BaseModel):
    version: int = Field(..., description="Số phiên bản (VD: 1, 2, 3)")
    created_at: str = Field(..., description="Thời gian tạo phiên bản")
    created_by: str = Field(..., description="Người tạo/cập nhật")
    change_summary: str = Field(..., description="Tóm tắt thay đổi")


class DatasetResponse(BaseModel):
    id: str
    dataset_name: str
    display_name: str
    domain: str
    description: Optional[str] = None
    primary_keys: List[str]
    current_version: int
    mappings: List[ColumnMapping]
    dq_rules: List[DataQualityRule]
    version_history: List[SchemaVersionInfo]
