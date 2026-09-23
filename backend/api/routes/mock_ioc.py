# -*- coding: utf-8 -*-
"""
api/routes/mock_ioc.py
------------------------------------------------------------
Router FastAPI Giả lập (Mock API) cổng CTU SMART IOC (Đại học Cần Thơ)
Phục vụ kiểm thử kết nối HTTP Adapter và Auto Schema Inference từ xa.
"""

from typing import List, Dict, Any
from fastapi import APIRouter

router = APIRouter()

# Dữ liệu giả lập 1: Kết quả học tập theo chương trình (CTU Smart IOC)
MOCK_LEARNING_OUTCOMES_DATA = [
    {"stt": 1, "chuong_trinh": "Kỹ thuật xây dựng", "sv_theo_hoc": "1.284", "qua_hp": "84,8%", "gpa": "2,74", "canh_bao": 38, "nguy_co_nghi": 22, "dung_tien_do": "76,2%", "xu_huong": "Giảm 3,1%"},
    {"stt": 2, "chuong_trinh": "Công nghệ kỹ thuật hóa học", "sv_theo_hoc": "862", "qua_hp": "86,1%", "gpa": "2,81", "canh_bao": 31, "nguy_co_nghi": 18, "dung_tien_do": "78,5%", "xu_huong": "Giảm 1,8%"},
    {"stt": 3, "chuong_trinh": "Khoa học máy tính", "sv_theo_hoc": "1.418", "qua_hp": "88,6%", "gpa": "2,96", "canh_bao": 29, "nguy_co_nghi": 16, "dung_tien_do": "82,7%", "xu_huong": "Ổn định"},
    {"stt": 4, "chuong_trinh": "Luật", "sv_theo_hoc": "2.106", "qua_hp": "89,2%", "gpa": "3,02", "canh_bao": 27, "nguy_co_nghi": 15, "dung_tien_do": "84,1%", "xu_huong": "Tăng 0,6%"},
    {"stt": 5, "chuong_trinh": "Quản trị kinh doanh", "sv_theo_hoc": "2.384", "qua_hp": "90,4%", "gpa": "3,06", "canh_bao": 24, "nguy_co_nghi": 13, "dung_tien_do": "85,8%", "xu_huong": "Tăng 1,2%"},
    {"stt": 6, "chuong_trinh": "Công nghệ thực phẩm", "sv_theo_hoc": "1.026", "qua_hp": "91,8%", "gpa": "3,11", "canh_bao": 19, "nguy_co_nghi": 10, "dung_tien_do": "87,3%", "xu_huong": "Tăng 1,5%"},
    {"stt": 7, "chuong_trinh": "Ngôn ngữ Anh", "sv_theo_hoc": "1.176", "qua_hp": "92,7%", "gpa": "3,18", "canh_bao": 17, "nguy_co_nghi": 8, "dung_tien_do": "89,6%", "xu_huong": "Tăng 2,1%"},
    {"stt": 8, "chuong_trinh": "Nuôi trồng thủy sản", "sv_theo_hoc": "918", "qua_hp": "93,4%", "gpa": "3,22", "canh_bao": 14, "nguy_co_nghi": 7, "dung_tien_do": "90,8%", "xu_huong": "Tăng 2,4%"},
    {"stt": 9, "chuong_trinh": "Công nghệ sinh học", "sv_theo_hoc": "794", "qua_hp": "94,1%", "gpa": "3,26", "canh_bao": 11, "nguy_co_nghi": 5, "dung_tien_do": "92,2%", "xu_huong": "Tăng 2,8%"},
    {"stt": 10, "chuong_trinh": "Sư phạm Toán học", "sv_theo_hoc": "486", "qua_hp": "95,6%", "gpa": "3,34", "canh_bao": 6, "nguy_co_nghi": 2, "dung_tien_do": "94,7%", "xu_huong": "Tăng 3,2%"},
]

# Dữ liệu giả lập 2: Tiến độ giảng dạy theo đơn vị đào tạo (CTU Smart IOC)
MOCK_TEACHING_PROGRESS_DATA = [
    {"stt": 1, "don_vi_dao_tao": "Trường Bách khoa", "lop_hp": 684, "dung_tien_do": "92,8%", "hien_dien": "91,6%", "nhap_diem": "88,4%", "doi_lich": 18, "phan_hoi_sv": "4,21/5", "danh_gia": "Cần cải thiện"},
    {"stt": 2, "don_vi_dao_tao": "Trường Khoa học Tự nhiên", "lop_hp": 426, "dung_tien_do": "93,7%", "hien_dien": "92,4%", "nhap_diem": "90,1%", "doi_lich": 11, "phan_hoi_sv": "4,28/5", "danh_gia": "Cần theo dõi"},
    {"stt": 3, "don_vi_dao_tao": "Trường Kinh tế", "lop_hp": 712, "dung_tien_do": "95,2%", "hien_dien": "93,8%", "nhap_diem": "91,7%", "doi_lich": 9, "phan_hoi_sv": "4,32/5", "danh_gia": "Cần theo dõi"},
    {"stt": 4, "don_vi_dao_tao": "Trường CNTT&TT", "lop_hp": 498, "dung_tien_do": "96,1%", "hien_dien": "94,2%", "nhap_diem": "92,6%", "doi_lich": 8, "phan_hoi_sv": "4,38/5", "danh_gia": "Đạt"},
    {"stt": 5, "don_vi_dao_tao": "Trường Nông nghiệp", "lop_hp": 516, "dung_tien_do": "96,8%", "hien_dien": "94,9%", "nhap_diem": "94,1%", "doi_lich": 6, "phan_hoi_sv": "4,41/5", "danh_gia": "Đạt"},
    {"stt": 6, "don_vi_dao_tao": "Trường Sư phạm", "lop_hp": 438, "dung_tien_do": "97,4%", "hien_dien": "95,6%", "nhap_diem": "95,2%", "doi_lich": 5, "phan_hoi_sv": "4,47/5", "danh_gia": "Tốt"},
    {"stt": 7, "don_vi_dao_tao": "Trường Thủy sản", "lop_hp": 306, "dung_tien_do": "97,8%", "hien_dien": "95,9%", "nhap_diem": "95,8%", "doi_lich": 4, "phan_hoi_sv": "4,52/5", "danh_gia": "Tốt"},
    {"stt": 8, "don_vi_dao_tao": "Khoa Ngoại ngữ", "lop_hp": 348, "dung_tien_do": "98,1%", "hien_dien": "96,2%", "nhap_diem": "96,4%", "doi_lich": 3, "phan_hoi_sv": "4,55/5", "danh_gia": "Tốt"},
    {"stt": 9, "don_vi_dao_tao": "Trường KHXH&NV", "lop_hp": 274, "dung_tien_do": "98,4%", "hien_dien": "96,8%", "nhap_diem": "97,1%", "doi_lich": 2, "phan_hoi_sv": "4,58/5", "danh_gia": "Tốt"},
    {"stt": 10, "don_vi_dao_tao": "Viện CNSH&TP", "lop_hp": 184, "dung_tien_do": "98,9%", "hien_dien": "97,1%", "nhap_diem": "97,6%", "doi_lich": 1, "phan_hoi_sv": "4,61/5", "danh_gia": "Xuất sắc"},
]


@router.get("/mock/ctu-ioc/learning-outcomes", summary="[MOCK CTU IOC] API Giả lập Kết quả học tập")
def get_mock_learning_outcomes() -> List[Dict[str, Any]]:
    """Trả về mảng JSON giả lập từ cổng CTU SMART IOC về Kết quả học tập theo chương trình."""
    return MOCK_LEARNING_OUTCOMES_DATA


@router.get("/mock/ctu-ioc/teaching-progress", summary="[MOCK CTU IOC] API Giả lập Tiến độ giảng dạy")
def get_mock_teaching_progress() -> List[Dict[str, Any]]:
    """Trả về mảng JSON giả lập từ cổng CTU SMART IOC về Tiến độ giảng dạy theo đơn vị."""
    return MOCK_TEACHING_PROGRESS_DATA
