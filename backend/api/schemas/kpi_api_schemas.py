from pydantic import BaseModel, Field
from typing import Optional, Any

class TeachingProgressPayload(BaseModel):
    ky_danh_gia: Optional[str] = Field(None, description="Kỳ đánh giá (Ví dụ: Q2/2026)")
    don_vi_dao_tao: Optional[str] = Field(None, description="Tên khoa hoặc đơn vị đào tạo")
    lop_hp: Optional[int] = Field(None, description="Số lượng lớp học phần")
    dung_tien_do_pct: Optional[float] = Field(None, description="Tỷ lệ đúng tiến độ")
    hien_dien_pct: Optional[float] = Field(None, description="Tỷ lệ hiện diện")
    nhap_diem_pct: Optional[float] = Field(None, description="Tỷ lệ nhập điểm")
    doi_lich: Optional[int] = Field(None, description="Số lượng đổi lịch")
    diem_phan_hoi_sv: Optional[str] = Field(None, description="Điểm phản hồi sinh viên")
    danh_gia: Optional[str] = Field(None, description="Đánh giá phân loại")

    class Config:
        extra = 'allow'  # Cho phép nhận thêm bất kỳ trường mới nào không có trong danh sách trên
        
class LearningOutcomesPayload(BaseModel):
    ky_danh_gia: Optional[str] = Field(None, description="Kỳ đánh giá")
    chuong_trinh: Optional[str] = Field(None, description="Tên chương trình đào tạo")
    sv_theo_hoc: Optional[int] = Field(None, description="Tổng số sinh viên theo học")
    qua_hp_pct: Optional[float] = Field(None, description="Tỷ lệ qua học phần")
    gpa_trung_binh: Optional[float] = Field(None, description="Điểm trung bình GPA")
    can_bao_hoc_vu: Optional[int] = Field(None, description="Số lượng sinh viên cảnh báo")
    nguy_co_nghi_hoc: Optional[int] = Field(None, description="Số lượng sinh viên nguy cơ nghỉ")
    dung_tien_do_pct: Optional[float] = Field(None, description="Tỷ lệ đúng tiến độ")
    xu_huong: Optional[str] = Field(None, description="Xu hướng so với kỳ trước")

    class Config:
        extra = 'allow'