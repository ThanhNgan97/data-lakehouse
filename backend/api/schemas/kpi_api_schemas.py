from pydantic import BaseModel, Field
from typing import Optional

class TeachingProgressPayload(BaseModel):
    ky_danh_gia: str = Field(..., description="Kỳ đánh giá (Ví dụ: Q2/2026)")
    don_vi_dao_tao: str = Field(..., description="Tên khoa hoặc đơn vị đào tạo")
    lop_hp: int = Field(..., ge=0, description="Số lượng lớp học phần")
    dung_tien_do_pct: float = Field(..., ge=0.0, le=100.0, description="Tỷ lệ đúng tiến độ")
    hien_dien_pct: float = Field(..., ge=0.0, le=100.0, description="Tỷ lệ hiện diện")
    nhap_diem_pct: float = Field(..., ge=0.0, le=100.0, description="Tỷ lệ nhập điểm")
    doi_lich: int = Field(..., ge=0, description="Số lượng đổi lịch")
    diem_phan_hoi_sv: str = Field(..., description="Điểm phản hồi sinh viên dạng x.xx/5")
    danh_gia: str = Field(..., description="Đánh giá phân loại")

class LearningOutcomesPayload(BaseModel):
    ky_danh_gia: str = Field(..., description="Kỳ đánh giá")
    chuong_trinh: str = Field(..., description="Tên chương trình đào tạo")
    sv_theo_hoc: int = Field(..., ge=0, description="Tổng số sinh viên theo học")
    qua_hp_pct: float = Field(..., ge=0.0, le=100.0, description="Tỷ lệ qua học phần")
    gpa_trung_binh: float = Field(..., ge=0.0, le=4.0, description="Điểm trung bình GPA")
    can_bao_hoc_vu: int = Field(..., ge=0, description="Số lượng sinh viên cảnh báo")
    nguy_co_nghi_hoc: int = Field(..., ge=0, description="Số lượng sinh viên nguy cơ nghỉ")
    dung_tien_do_pct: float = Field(..., ge=0.0, le=100.0, description="Tỷ lệ đúng tiến độ")
    xu_huong: str = Field(..., description="Xu hướng so với kỳ trước")