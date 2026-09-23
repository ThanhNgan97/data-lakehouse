# -*- coding: utf-8 -*-
"""
api/routes/registry.py
------------------------------------------------------------
Router FastAPI: Quản lý Dataset Registry
Cung cấp API cho Schema Definition, Column Mapping, Primary Keys,
Data Quality (DQ Rules) và Versioning theo đúng sơ đồ kiến trúc Lakehouse.
"""

from typing import List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_current_user
from api.schemas.registry import (
    DatasetCreate,
    DatasetResponse,
    ColumnMapping,
    DataQualityRule,
    SchemaVersionInfo,
)

router = APIRouter()

# Bộ nhớ tạm phục vụ lưu trữ Dataset Registry (API-first, sẵn sàng đồng bộ DB sau)
MOCK_DATASETS_REGISTRY = {
    "kpi_cusc": DatasetResponse(
        id="ds-kpi-001",
        dataset_name="kpi_cusc",
        display_name="Tổng hợp & Chi tiết KPI Đơn vị",
        domain="kpi",
        description="Dataset quản lý tiến độ thực hiện chỉ tiêu KPI của các đơn vị phòng ban.",
        primary_keys=["ma_chi_tieu", "nhom_don_vi", "ky_thu_thap"],
        current_version=1,
        mappings=[
            ColumnMapping(source_column="Mã chỉ tiêu", target_column="ma_chi_tieu", data_type="string", is_primary_key=True, is_nullable=False, description="Mã định danh chỉ tiêu"),
            ColumnMapping(source_column="Tên chỉ tiêu", target_column="ten_chi_tieu", data_type="string", is_primary_key=False, is_nullable=True, description="Tên mô tả chỉ tiêu KPI"),
            ColumnMapping(source_column="Mã ĐV", target_column="nhom_don_vi", data_type="string", is_primary_key=True, is_nullable=False, description="Mã đơn vị/phòng ban"),
            ColumnMapping(source_column="Kỳ", target_column="ky_thu_thap", data_type="string", is_primary_key=True, is_nullable=False, description="Kỳ thu thập dữ liệu"),
            ColumnMapping(source_column="Giá trị thực hiện", target_column="gia_tri_thuc_hien", data_type="float", is_primary_key=False, is_nullable=True, description="Giá trị đo lường thực tế"),
        ],
        dq_rules=[
            DataQualityRule(rule_id="DQ_KPI_01", rule_name="Not Null Key", rule_type="NOT_NULL", column_name="ma_chi_tieu", parameters={}, severity="ERROR"),
            DataQualityRule(rule_id="DQ_KPI_02", rule_name="Positive Score", rule_type="VALUE_RANGE", column_name="gia_tri_thuc_hien", parameters={"min": 0}, severity="WARNING"),
        ],
        version_history=[
            SchemaVersionInfo(version=1, created_at=datetime.now().isoformat(), created_by="admin", change_summary="Khởi tạo Schema ban đầu cho KPI CUSC"),
        ],
    ),
    "learning_outcomes": DatasetResponse(
        id="ds-learn-002",
        dataset_name="learning_outcomes",
        display_name="Kết quả Học tập Sinh viên",
        domain="learning",
        description="Dataset kết quả học tập, điểm số và chứng chỉ của người học.",
        primary_keys=["student_id", "course_code", "semester"],
        current_version=1,
        mappings=[
            ColumnMapping(source_column="MSSV", target_column="student_id", data_type="string", is_primary_key=True, is_nullable=False, description="Mã số sinh viên"),
            ColumnMapping(source_column="Mã môn", target_column="course_code", data_type="string", is_primary_key=True, is_nullable=False, description="Mã môn học"),
            ColumnMapping(source_column="Học kỳ", target_column="semester", data_type="string", is_primary_key=True, is_nullable=False, description="Học kỳ áp dụng"),
            ColumnMapping(source_column="Điểm số", target_column="gpa_score", data_type="float", is_primary_key=False, is_nullable=True, description="Điểm số học phần"),
        ],
        dq_rules=[
            DataQualityRule(rule_id="DQ_LRN_01", rule_name="Valid Student ID", rule_type="REGEX_MATCH", column_name="student_id", parameters={"pattern": "^[A-Z0-9]{8,10}$"}, severity="ERROR"),
        ],
        version_history=[
            SchemaVersionInfo(version=1, created_at=datetime.now().isoformat(), created_by="admin", change_summary="Khởi tạo Schema cho Kết quả học tập"),
        ],
    ),
    "teaching_progress": DatasetResponse(
        id="ds-teach-003",
        dataset_name="teaching_progress",
        display_name="Tiến độ Giảng dạy Giảng viên",
        domain="teaching",
        description="Dataset theo dõi khối lượng và tiến độ giảng dạy của giảng viên.",
        primary_keys=["lecturer_id", "class_code"],
        current_version=1,
        mappings=[
            ColumnMapping(source_column="Mã GV", target_column="lecturer_id", data_type="string", is_primary_key=True, is_nullable=False, description="Mã giảng viên"),
            ColumnMapping(source_column="Mã Lớp", target_column="class_code", data_type="string", is_primary_key=True, is_nullable=False, description="Mã lớp học phần"),
            ColumnMapping(source_column="Số tiết hoàn thành", target_column="completed_hours", data_type="integer", is_primary_key=False, is_nullable=True, description="Số tiết đã dạy"),
        ],
        dq_rules=[],
        version_history=[
            SchemaVersionInfo(version=1, created_at=datetime.now().isoformat(), created_by="admin", change_summary="Khởi tạo Schema Tiến độ Giảng dạy"),
        ],
    ),
}


@router.get("/registry/datasets", response_model=List[DatasetResponse], summary="Lấy danh sách tất cả Datasets trong Registry")
def list_datasets(
    domain: str | None = Query(None, description="Lọc theo miền dữ liệu ('learning', 'teaching', 'kpi')"),
    current_user: dict = Depends(get_current_user),
):
    """Trả về toàn bộ danh sách các Dataset đã được đăng ký trong Dataset Registry."""
    datasets = list(MOCK_DATASETS_REGISTRY.values())
    if domain:
        datasets = [ds for ds in datasets if ds.domain.lower() == domain.lower()]
    return datasets


@router.get("/registry/datasets/{dataset_name}", response_model=DatasetResponse, summary="Lấy chi tiết 1 Dataset Registry")
def get_dataset_details(
    dataset_name: str,
    current_user: dict = Depends(get_current_user),
):
    """Lấy chi tiết Schema, Mapping cột, Khóa chính và các luật Data Quality (DQ Rules) của 1 Dataset."""
    if dataset_name not in MOCK_DATASETS_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy Dataset '{dataset_name}' trong Registry.")
    return MOCK_DATASETS_REGISTRY[dataset_name]


@router.post("/registry/datasets", response_model=DatasetResponse, summary="Đăng ký mới hoặc Cập nhật Schema 1 Dataset")
def create_or_update_dataset(
    payload: DatasetCreate,
    current_user: dict = Depends(get_current_user),
):
    """
    Đăng ký mới một Dataset hoặc nâng version cho Schema hiện tại.
    Spark Silver Engine sẽ đọc cấu hình này để thực hiện Normalize / DQ / MERGE.
    """
    dataset_name = payload.dataset_name
    is_existing = dataset_name in MOCK_DATASETS_REGISTRY
    
    current_version = 1
    version_history = []
    
    if is_existing:
        old_ds = MOCK_DATASETS_REGISTRY[dataset_name]
        current_version = old_ds.current_version + 1
        version_history = old_ds.version_history
    
    new_version_info = SchemaVersionInfo(
        version=current_version,
        created_at=datetime.now().isoformat(),
        created_by=current_user.get("username", "system") if isinstance(current_user, dict) else getattr(current_user, "username", "admin"),
        change_summary=f"Nâng cấp Schema lên phiên bản v{current_version}",
    )
    version_history.append(new_version_info)

    ds_response = DatasetResponse(
        id=f"ds-{payload.domain}-{len(MOCK_DATASETS_REGISTRY) + 1:03d}",
        dataset_name=payload.dataset_name,
        display_name=payload.display_name,
        domain=payload.domain,
        description=payload.description,
        primary_keys=payload.primary_keys,
        current_version=current_version,
        mappings=payload.mappings,
        dq_rules=payload.dq_rules,
        version_history=version_history,
    )

    MOCK_DATASETS_REGISTRY[dataset_name] = ds_response
    return ds_response


@router.post("/registry/datasets/{dataset_name}/mapping", response_model=DatasetResponse, summary="Cập nhật quy tắc Mapping cột")
def update_column_mapping(
    dataset_name: str,
    mappings: List[ColumnMapping],
    current_user: dict = Depends(get_current_user),
):
    """Cập nhật hoặc thêm mới các quy tắc Mapping cột từ Bronze sang Silver cho Dataset."""
    if dataset_name not in MOCK_DATASETS_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy Dataset '{dataset_name}'.")

    ds = MOCK_DATASETS_REGISTRY[dataset_name]
    ds.mappings = mappings
    ds.current_version += 1
    ds.version_history.append(SchemaVersionInfo(
        version=ds.current_version,
        created_at=datetime.now().isoformat(),
        created_by="admin",
        change_summary="Cập nhật cấu hình Column Mapping",
    ))
    return ds


@router.post("/registry/datasets/{dataset_name}/dq-rules", response_model=DatasetResponse, summary="Cập nhật các luật Data Quality (DQ Rules)")
def update_dq_rules(
    dataset_name: str,
    rules: List[DataQualityRule],
    current_user: dict = Depends(get_current_user),
):
    """Cập nhật danh sách các luật kiểm tra chất lượng dữ liệu (Data Quality) cho Dataset."""
    if dataset_name not in MOCK_DATASETS_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy Dataset '{dataset_name}'.")

    ds = MOCK_DATASETS_REGISTRY[dataset_name]
    ds.dq_rules = rules
    return ds
