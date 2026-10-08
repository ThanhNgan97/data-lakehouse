# -*- coding: utf-8 -*-
"""Template-Driven Dynamic Superset Provisioner for Universal Data Lakehouse.

This module automates the entire Superset BI plane from pipeline RoutingDecisions:
1. Slot-filling: Fills schema metadata (metrics, dimensions, time) into Visual Archetype templates.
2. Rich Vietnamese Semantics: Normalizes field names into natural Vietnamese with diacritics and enforces acronym capitalization (TCDK, TB, TBRL, GPA, SV).
3. Layout Rules:
   - Rule 1: KPI number cards are strictly prioritized at the top (Row 1).
   - Rule 2: Visual charts (Donut, Top 10 Bar) are placed in the middle (Row 2).
   - Rule 3: Detail data tables are ALWAYS placed at the very bottom (Row 3).
4. Enforces deterministic UUIDs (idempotency, no duplicate dashboard sprawl).
5. Packages into standard Superset ZIP manifest and imports via REST API (primary) or Docker CLI (fallback).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Set
import uuid
import zipfile
import requests
import yaml

# Đảm bảo console Windows in tiếng Việt UTF-8 không bị lỗi charmap
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_CURRENT_DIR = Path(__file__).resolve().parent
if str(_CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CURRENT_DIR))

from ai_dataset_router import RoutingDecision, to_snake_case
from relational_context import is_technical_column

DATABASE_UUID = "41989b72-5069-4671-8a40-78245bd3bd30"  # CTU IOC Trino UUID
NAMESPACE_SUPERSET = uuid.NAMESPACE_DNS


# =====================================================================
# BỘ TỪ ĐIỂN VÀ CHUẨN HÓA TIẾNG VIỆT NGỮ NGHĨA & VIẾT TẮT
# =====================================================================

KNOWN_ACRONYMS = [
    "TCDK", "TBRL", "TB", "RL", "GPA", "MSSV", "SV", "CTU", "IOC",
    "K48", "K47", "K49", "NCKH", "HP", "CNTT", "KT", "LK", "FL",
    "DA", "TS", "SP", "MT", "NN", "KH", "DI",
    "ATK", "DEF", "DPS", "C6", "C0", "C1", "C2", "C3", "C4", "C5", "C7",
    "CRIT", "DMG", "AR", "UI", "UX", "API", "ID", "3D",
    "SLA", "KPI", "MOM", "YOY", "WOW", "TIER", "CUSC"
]

VIETNAMESE_DICTIONARY: Dict[str, str] = {
    # Định danh & con người
    "ma_sv": "Mã SV",
    "mssv": "MSSV",
    "student_id": "Mã SV",
    "student_code": "Mã SV",
    "ho_ten": "Họ và Tên",
    "student_name": "Họ và Tên",
    "full_name": "Họ và Tên",
    "ngay_sinh": "Ngày Sinh",
    "gioi_tinh": "Giới Tính",
    "name": "Tên Nhân Vật",
    # Đào tạo & trường lớp
    "lop": "Lớp",
    "class": "Lớp",
    "don_vi": "Đơn Vị",
    "khoa": "Khoa / Viện",
    "faculty": "Khoa / Viện",
    "nganh": "Ngành Đào Tạo",
    "major": "Ngành Đào Tạo",
    "chuyen_nganh": "Chuyên Ngành",
    "academic_year": "Năm Học",
    "nam_hoc": "Năm Học",
    "hoc_ky": "Học Kỳ",
    "semester": "Học Kỳ",
    # Thành tích, điểm số & khen thưởng (TCDK, TB, TBRL viết hoa toàn bộ)
    "xep_loai": "Xếp Loại Khen Thưởng",
    "so_tcdk": "Số TCDK",
    "tcdk": "Số TCDK",
    "tin_chi": "Số Tín Chỉ",
    "credits": "Số Tín Chỉ",
    "diem_tb": "Điểm TB",
    "gpa": "Điểm GPA",
    "diem_tbrl": "Điểm TBRL",
    "tbrl": "Điểm TBRL",
    "drl": "Điểm RL",
    "khen_thuong": "Khen Thưởng",
    "hoc_bong": "Học Bổng",
    "so_quyet_dinh": "Số Quyết Định",
    "ngay_ky": "Ngày Ký",
    # Tài chính & học phí
    "amount_paid": "Số Tiền Đã Đóng",
    "tuition_amount": "Mức Học Phí",
    "hoc_phi": "Học Phí",
    "so_tien": "Số Tiền",
    "paid_at": "Ngày Đóng Học Phí",
    "payment_date": "Ngày Giao Dịch",
    # Bản ghi & hệ thống
    "records": "Số Bản Ghi",
    "total_records": "Tổng Số Bản Ghi",
    "trang_thai": "Trạng Thái",
    "status": "Trạng Thái",
    "ghi_chu": "Ghi Chú",
    # Genshin Impact & Game Analytics Domain
    "rarity": "Độ Hiếm",
    "weapon": "Vũ Khí",
    "element": "Nguyên Tố",
    "region": "Vùng Đất",
    "release_date": "Ngày Phát Hành",
    "pulled_count": "Lượt Pulls",
    "lvl_90_hp": "HP Cấp 90",
    "lvl_90_atk": "ATK Cấp 90",
    "lvl_90_def": "DEF Cấp 90",
    "duplicate_rate": "Tỷ Lệ Trùng Lặp",
    "c6_rate": "Tỷ Lệ C6",
    "avg_copies_per_player": "Số Bản Sao TB",
    "num_banners": "Số Lượng Banner",
    "months_since_release": "Số Tháng Ra Mắt",
    "days_since_release": "Số Ngày Ra Mắt",
    "roles": "Vai Trò",
    "asc_stat_bonus_category": "Chỉ Số Đột Phá",
    "is_standard_banner": "Banner Tiêu Chuẩn",
    "is_archon": "Archon",
    "3d_model": "Mô Hình 3D",
    "3_d_model": "Mô Hình 3D",
    "banner_versions": "Phiên Bản Banner",
    "banner_dates": "Ngày Banner",
    "constellation_0_pull": "Lượt Pull C0",
    "constellation_1_pull": "Lượt Pull C1",
    "constellation_2_pull": "Lượt Pull C2",
    "constellation_3_pull": "Lượt Pull C3",
    "constellation_4_pull": "Lượt Pull C4",
    "constellation_5_pull": "Lượt Pull C5",
    "constellation_6_pull": "Lượt Pull C6",
    "constellation_7_pull": "Lượt Pull C7",
    # Nhóm KPI CUSC Domain & Đánh giá hiệu suất
    "nhom_don_vi": "Nhóm Đơn Vị",
    "quy_danh_gia": "Kỳ Đánh Giá",
    "ma_chi_tieu": "Mã Chỉ Tiêu",
    "noi_dung_muc_tieu": "Nội Dung Mục Tiêu",
    "muc_dang_ky": "Mức Đăng Ký",
    "muc_dang_ky_numeric": "Chỉ Số Đăng Ký",
    "muc_dat": "Mức Thực Đạt",
    "muc_dat_numeric": "Chỉ Số Thực Đạt",
    "ket_qua_he_thong": "Kết Quả Hệ Thống",
    "ty_le_hoan_thanh_phan_tram": "Tỷ Lệ Hoàn Thành (%)",
    "tong_chi_tieu_danh_gia": "Tổng Số Chỉ Tiêu",
    "so_chi_tieu_dat": "Số Chỉ Tiêu Đạt",
    "so_chi_tieu_khong_dat": "Số Chỉ Tiêu Không Đạt",
    "dinh_ky_thu_thap": "Định Kỳ Thu Thập",
    "hanh_dong_khac_phuc": "Hành Động Khắc Phục",
    "ten_phong_ban": "Tên Phòng Ban",
    "thoi_gian_cap_nhat": "Thời Gian Cập Nhật",
    "thoi_gian_dong_goi_gold": "Thời Gian Đóng Gói Gold",
    "_gold_generated_at": "Thời Gian Khởi Tạo Gold",
    "ty_le_hien_dien": "Tỷ Lệ Hiện Diện (%)",
    "tong_luot_diem_danh": "Tổng Lượt Điểm Danh",
    "so_luot_co_mat": "Số Lượt Có Mặt",
    "ty_le_dung_tien_do": "Tỷ Lệ Đúng Tiến Độ (%)",
    "ty_le_nhap_diem": "Tỷ Lệ Nhập Điểm (%)",
    "diem_danh_gia": "Điểm Đánh Giá",
    "muc_do_hai_long": "Mức Độ Hài Lòng",
    "so_sv_khao_sat": "Số SV Khảo Sát",
    "ma_lop_hp": "Mã Lớp HP",
    "ten_mon_hoc": "Tên Môn Học",
    "ten_don_vi": "Tên Đơn Vị",
    "loai_don_vi": "Loại Đơn Vị",
    # Nhóm Decision-driven Fields (Chuẩn superset-new-implement.md)
    "action_priority": "Mức Độ Ưu Tiên Xử Lý",
    "health_status": "Trạng Thái Sức Khỏe Nghiệp Vụ",
    "performance_tier": "Phân Khúc Hiệu Suất (Pareto)",
    "recommended_action": "Khuyến Nghị Hành Động Nghiệp Vụ",
    "alert_flag": "Cờ Cảnh Báo Nguy Cơ",
    "risk_score": "Điểm Đánh Giá Rủi Ro",
    "target_value": "Mục Tiêu Định Mức",
    "target_achievement_pct": "Tỷ Lệ Đạt Mục Tiêu (%)",
    "target_variance_amount": "Độ Lệch Mục Tiêu",
    "aging_days": "Số Ngày Kể Từ Mốc Sự Kiện",
    # IoT Telemetry & Environmental Sensors Domain
    "device": "Mã Thiết Bị",
    "co": "Nồng Độ Khí CO",
    "humidity": "Độ Ẩm (%)",
    "light": "Trạng Thái Ánh Sáng",
    "lpg": "Nồng Độ Gas LPG",
    "motion": "Cảm Biến Chuyển Động",
    "smoke": "Nồng Độ Khói",
    "temp": "Nhiệt Độ (°C)",
    "ts": "Mốc Thời Gian",
}

ENTITY_NAME_MAP: Dict[str, str] = {
    "student_awards_k48": "Khen Thưởng Sinh Viên Khóa 48",
    "student_tuition": "Học Phí Sinh Viên",
    "dynamic_sample_tuition_json": "Học Phí Sinh Viên",
    "learning_outcomes": "Kết Quả Học Tập",
    "teaching_progress": "Tiến Độ Giảng Dạy",
    "genshin_impact_character_stats": "Nhân Vật Genshin Impact",
    "genshin_impact": "Genshin Impact",
    "kpi_cusc_master": "Chỉ Số KPI CUSC",
    "kpi_tong_hop_don_vi": "Tổng Hợp KPI CUSC",
    "iot_telemetry": "Dữ Liệu Đo Lường IoT & Cảm Biến",
    "iot_telemetry_summary": "Tổng Hợp Đo Lường Cảm Biến IoT",
    "ctu_ioc_test_context": "Tổng Quan Điều Hành Đào Tạo CTU IOC",
    "dm_don_vi_dao_tao": "Danh Mục Đơn Vị Đào Tạo",
    "diem_danh_lop_hp": "Điểm Danh Lớp HP",
    "tien_do_giang_day": "Tiến Độ Giảng Dạy",
    "tien_do_nhap_diem": "Tiến Độ Nhập Điểm",
    "khao_sat_sinh_vien": "Khảo Sát Sinh Viên",
    "doi_lich_giang_day": "Đổi Lịch Giảng Dạy",
}


def capitalize_abbreviations(text: str) -> str:
    """Tự động phát hiện và viết hoa toàn bộ các từ viết tắt theo đúng quy tắc nghiệp vụ."""
    pattern = r'\b(' + '|'.join(re.escape(a) for a in KNOWN_ACRONYMS) + r')\b'
    return re.sub(pattern, lambda m: m.group(1).upper(), text, flags=re.IGNORECASE)


def to_vietnamese_label(field: str) -> str:
    """Chuyển đổi tên trường snake_case thành nhãn tiếng Việt có dấu chuẩn xác và viết hoa viết tắt."""
    raw = field.strip().lower()

    # Relational Gold fields use <entity>__<aggregation>__<business_field>.
    # Render the business meaning first and keep the source entity as context.
    if "__" in raw:
        parts = raw.split("__")
        entity = parts[0]
        if len(parts) >= 3 and parts[1] in {"sum", "avg"}:
            business_field = "__".join(parts[2:])
            base = to_vietnamese_label(business_field)
            aggregate = "Tổng" if parts[1] == "sum" else "Bình Quân"
            return capitalize_abbreviations(
                f"{aggregate} {base} – {to_vietnamese_label(entity)}"
            )
        if len(parts) == 2 and parts[1] == "record_count":
            return capitalize_abbreviations(f"Số Bản Ghi – {to_vietnamese_label(entity)}")
        if len(parts) == 2:
            return capitalize_abbreviations(
                f"{to_vietnamese_label(parts[1])} – {to_vietnamese_label(entity)}"
            )

    if raw in ENTITY_NAME_MAP:
        return capitalize_abbreviations(ENTITY_NAME_MAP[raw])
    if raw in VIETNAMESE_DICTIONARY:
        return capitalize_abbreviations(VIETNAMESE_DICTIONARY[raw])

    # Xử lý tiền tố tổng hợp sum_ / avg_
    if raw.startswith("sum_"):
        inner = raw[4:]
        base = VIETNAMESE_DICTIONARY.get(inner, inner.replace("_", " ").title())
        return capitalize_abbreviations(f"Tổng {base}")
    elif raw.startswith("avg_"):
        inner = raw[4:]
        if any(k in inner for k in ["diem", "gpa", "atk", "hp", "def"]):
            base = VIETNAMESE_DICTIONARY.get(inner, inner.replace("_", " ").title())
            return capitalize_abbreviations(f"{base} (Bình Quân)")
        base = VIETNAMESE_DICTIONARY.get(inner, inner.replace("_", " ").title())
        return capitalize_abbreviations(f"TB {base}")

    # Tách từng token để dịch ghép
    parts = raw.split("_")
    translated = []
    for p in parts:
        if p == "co" and raw != "co":
            translated.append("Có")
        elif p in VIETNAMESE_DICTIONARY:
            translated.append(VIETNAMESE_DICTIONARY[p])
        elif p == "tb":
            translated.append("TB")
        elif p == "rl":
            translated.append("RL")
        elif p == "sv":
            translated.append("SV")
        elif p == "tcdk":
            translated.append("TCDK")
        elif p == "tbrl":
            translated.append("TBRL")
        elif p == "cdk":
            translated.append("TCDK")
        else:
            translated.append(p.capitalize())

    result = " ".join(translated)
    return capitalize_abbreviations(result)


def format_entity_title(entity_name: str) -> str:
    """Chuyển đổi tên thực thể dataset sang tiêu đề tiếng Việt chuẩn."""
    clean = entity_name.lower().strip()
    if clean in ENTITY_NAME_MAP:
        return ENTITY_NAME_MAP[clean]
    return to_vietnamese_label(clean)


def is_average_metric(metric_name: str) -> bool:
    """Kiểm tra xem metric này có phải là chỉ số điểm số/tỷ lệ cần tính AVG thay vì SUM hay không."""
    m_lower = metric_name.lower()
    return any(k in m_lower for k in [
        "diem", "gpa", "ty_le", "ti_le", "rate", "percent", "score", "avg_",
        "atk", "hp", "def", "copies", "stat",
        "temp", "nhiet_do", "humidity", "do_am", "lpg", "smoke", "ppm", "celsius",
        "__avg__",
    ])


def get_deterministic_uuid(key: str) -> str:
    """Sinh UUID tất định (v5) để đảm bảo tính bất biến (idempotent), không sinh trùng rác."""
    return str(uuid.uuid5(NAMESPACE_SUPERSET, f"ctu.lakehouse.{key}"))


def get_trino_columns(table_name: str, schema: str = "gold") -> Optional[List[str]]:
    """Truy vấn Trino REST API để lấy danh sách cột thực tế của bảng Gold."""
    candidates = [
        ("trino", 8080),
        ("localhost", 8081),
        ("127.0.0.1", 8081),
        ("localhost", 8080),
    ]
    for host, port in candidates:
        try:
            req = requests.post(
                f"http://{host}:{port}/v1/statement",
                data=f"DESCRIBE lakehouse.{schema}.{table_name}".encode("utf-8"),
                headers={
                    "X-Trino-User": "airflow",
                    "X-Trino-Catalog": "lakehouse",
                    "Content-Type": "text/plain; charset=utf-8",
                },
                timeout=3,
            )
            data = req.json()
            next_uri = data.get("nextUri")
            while next_uri:
                r = requests.get(next_uri, headers={"X-Trino-User": "airflow"}, timeout=3)
                data = r.json()
                next_uri = data.get("nextUri")
                if "data" in data and data["data"]:
                    return [row[0].lower() for row in data["data"]]
        except Exception:
            continue
    return None


# =====================================================================
# BUILDERS: DATASET & CHARTS
# =====================================================================

def build_dataset_yaml(decision: RoutingDecision) -> Tuple[Dict[str, Any], str]:
    """Sinh cấu hình Dataset YAML cho bảng Gold trong Trino kèm nhãn tiếng Việt chuẩn."""
    table_name = decision.target_gold_table.split(".")[-1]
    dataset_uuid = get_deterministic_uuid(f"dataset.{table_name}")
    real_cols = get_trino_columns(table_name, schema="gold")
    real_cols_set = set(real_cols) if real_cols else None

    columns_config = []
    metrics_config = [
        {
            "metric_name": "count",
            "verbose_name": "Tổng số bản ghi",
            "metric_type": "count",
            "expression": "COUNT(*)",
            "description": "Tổng số lượng dòng trong tập dữ liệu",
        }
    ]

    added_cols = set()

    def _col_valid(col_name: str) -> bool:
        return real_cols_set is None or col_name.lower() in real_cols_set

    # 1. Cấu hình Dimensions & Temporal
    for dim in decision.dimension_columns:
        raw_dim = dim.strip().lower()
        col_snake = raw_dim if real_cols_set and raw_dim in real_cols_set else to_snake_case(dim)
        if is_technical_column(col_snake) or col_snake in added_cols or not _col_valid(col_snake):
            continue
        added_cols.add(col_snake)
        is_dttm = bool(decision.source_updated_at_field and col_snake == to_snake_case(decision.source_updated_at_field))
        columns_config.append({
            "column_name": col_snake,
            "verbose_name": to_vietnamese_label(dim),
            "is_dttm": is_dttm,
            "is_active": True,
            "type": "DATE" if "date" in col_snake else "VARCHAR",
            "groupby": True,
            "filterable": True,
        })

    # 2. Cấu hình Temporal / Timestamp nếu chưa có trong dimension_columns
    main_dttm = None
    if decision.source_updated_at_field:
        main_dttm = to_snake_case(decision.source_updated_at_field)
        if is_technical_column(main_dttm) or not _col_valid(main_dttm):
            main_dttm = None

    if not main_dttm and real_cols_set:
        for fallback_dttm in ["thoi_gian_cap_nhat", "_gold_generated_at", "thoi_gian_dong_goi_gold", "release_date", "payment_date", "paid_at"]:
            if fallback_dttm in real_cols_set:
                main_dttm = fallback_dttm
                break

    if main_dttm and main_dttm not in added_cols and _col_valid(main_dttm):
        added_cols.add(main_dttm)
        v_name = to_vietnamese_label(main_dttm)
        columns_config.append({
            "column_name": main_dttm,
            "verbose_name": v_name,
            "is_dttm": True,
            "is_active": True,
            "type": "DATE" if "date" in main_dttm else "TIMESTAMP",
            "groupby": True,
            "filterable": True,
        })

    # Cột total_records luôn có trong Gold table
    if _col_valid("total_records"):
        added_cols.add("total_records")
        columns_config.append({
            "column_name": "total_records",
            "verbose_name": "Số Bản Ghi",
            "is_dttm": False,
            "is_active": True,
            "type": "BIGINT",
            "groupby": False,
            "filterable": True,
        })

    # 3. Cấu hình Metrics (Số liệu)
    for metric in decision.metric_columns:
        raw_metric = metric.strip().lower()
        m_snake = raw_metric if real_cols_set and raw_metric in real_cols_set else to_snake_case(metric)
        if is_technical_column(m_snake):
            continue
        m_vn = to_vietnamese_label(metric)
        is_avg = is_average_metric(metric)

        # Relational context metrics already exist as physical Gold columns.
        if _col_valid(m_snake):
            added_cols.add(m_snake)
            columns_config.append({
                "column_name": m_snake,
                "verbose_name": m_vn,
                "is_dttm": False,
                "is_active": True,
                "type": "DOUBLE",
                "groupby": False,
                "filterable": True,
            })
            metrics_config.append({
                "metric_name": f"metric_{m_snake}",
                "verbose_name": m_vn,
                "metric_type": "avg" if is_avg else "sum",
                "expression": f"{'AVG' if is_avg else 'SUM'}({m_snake})",
            })
            continue

        sum_col = f"sum_{m_snake}"
        if _col_valid(sum_col):
            added_cols.add(sum_col)
            columns_config.append({
                "column_name": sum_col,
                "verbose_name": f"Tổng {m_vn}",
                "is_dttm": False,
                "is_active": True,
                "type": "DOUBLE",
                "groupby": False,
                "filterable": True,
            })
            metrics_config.append({
                "metric_name": f"total_{m_snake}",
                "verbose_name": f"Tổng {m_vn}",
                "metric_type": "sum",
                "expression": f"SUM({sum_col})",
            })

        avg_col = f"avg_{m_snake}"
        if _col_valid(avg_col):
            added_cols.add(avg_col)
            columns_config.append({
                "column_name": avg_col,
                "verbose_name": f"{m_vn} (Bình Quân)" if is_avg else f"TB {m_vn}",
                "is_dttm": False,
                "is_active": True,
                "type": "DOUBLE",
                "groupby": False,
                "filterable": True,
            })
            metrics_config.append({
                "metric_name": f"avg_{m_snake}",
                "verbose_name": f"{m_vn} (Bình Quân)" if is_avg else f"TB {m_vn}",
                "metric_type": "avg",
                "expression": f"AVG({avg_col})",
            })

    # 4. Cấu hình Decision-driven Fields (Chuẩn superset-new-implement.md)
    decision_fields_def = [
        ("target_value", "Mục Tiêu Định Mức", "DOUBLE", False),
        ("target_achievement_pct", "Tỷ Lệ Đạt Mục Tiêu (%)", "DOUBLE", False),
        ("target_variance_amount", "Độ Lệch Mục Tiêu", "DOUBLE", False),
        ("performance_tier", "Phân Khúc Hiệu Suất (Pareto)", "VARCHAR", True),
        ("health_status", "Trạng Thái Sức Khỏe Nghiệp Vụ", "VARCHAR", True),
        ("action_priority", "Mức Độ Ưu Tiên Xử Lý", "VARCHAR", True),
        ("risk_score", "Điểm Đánh Giá Rủi Ro", "DOUBLE", False),
        ("alert_flag", "Cờ Cảnh Báo Nguy Cơ", "BOOLEAN", True),
        ("aging_days", "Số Ngày Kể Từ Mốc Sự Kiện", "INT", False),
        ("recommended_action", "Khuyến Nghị Hành Động Nghiệp Vụ", "VARCHAR", False),
    ]
    for col_name, verbose_name, col_type, can_groupby in decision_fields_def:
        if _col_valid(col_name):
            added_cols.add(col_name)
            columns_config.append({
                "column_name": col_name,
                "verbose_name": verbose_name,
                "is_dttm": False,
                "is_active": True,
                "type": col_type,
                "groupby": can_groupby,
                "filterable": True,
            })

    # Bổ sung bất kỳ cột thực tế nào còn lại từ bảng Trino
    if real_cols_set:
        for rc in real_cols_set:
            if rc not in added_cols and not rc.startswith("_") and not is_technical_column(rc):
                added_cols.add(rc)
                columns_config.append({
                    "column_name": rc,
                    "verbose_name": to_vietnamese_label(rc),
                    "is_dttm": "time" in rc or "date" in rc,
                    "is_active": True,
                    "type": "VARCHAR",
                    "groupby": True,
                    "filterable": True,
                })

    if _col_valid("health_status"):
        metrics_config.append({
            "metric_name": "count_critical",
            "verbose_name": "Cần Can Thiệp Khẩn (CRITICAL)",
            "metric_type": "count",
            "expression": "COUNT(CASE WHEN health_status = 'CRITICAL' THEN 1 END)",
        })
    if _col_valid("target_achievement_pct"):
        metrics_config.append({
            "metric_name": "avg_target_achievement",
            "verbose_name": "Tỷ Lệ Đạt Mục Tiêu (Bình Quân)",
            "metric_type": "avg",
            "expression": "AVG(target_achievement_pct)",
        })
    if _col_valid("risk_score"):
        metrics_config.append({
            "metric_name": "avg_risk_score",
            "verbose_name": "Điểm Rủi Ro (Bình Quân)",
            "metric_type": "avg",
            "expression": "AVG(risk_score)",
        })

    dataset_dict = {
        "table_name": table_name,
        "main_dttm_col": main_dttm,
        "description": f"AI Auto-Generated Data Mart: {decision.dataset_entity} ({decision.dataset_domain})",
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "schema": "gold",
        "sql": None,
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": False,
        "uuid": dataset_uuid,
        "metrics": metrics_config,
        "columns": columns_config,
        "database_uuid": DATABASE_UUID,
        "version": "1.0.0",
    }
    return dataset_dict, dataset_uuid


def build_chart_kpi(
    table_name: str,
    dataset_uuid: str,
    metric_name: str,
    metric_label: str,
    column_name: str = "total_records",
    aggregate: str = "COUNT",
    y_axis_format: str = ",.0f",
    adhoc_filters: Optional[List[Dict[str, Any]]] = None,
    sql_expression: Optional[str] = None,
) -> Tuple[Dict[str, Any], str]:
    """Sinh Chart Big Number KPI Card với tiếng Việt có dấu chuẩn và viết hoa viết tắt."""
    chart_uuid = get_deterministic_uuid(f"chart.kpi.{table_name}.{metric_name}")

    clean_label = capitalize_abbreviations(metric_label)
    if any(clean_label.lower().startswith(p) for p in ["tổng ", "tb ", "điểm ", "cần ", "mục tiêu ", "tỷ lệ ", "quy mô ", "số ", "đỉnh điểm ", "cao nhất ", "thấp nhất "]):
        slice_name = clean_label
    elif aggregate == "AVG" or "bình quân" in clean_label.lower():
        slice_name = f"TB {clean_label.replace(' (Bình Quân)', '')}"
    else:
        slice_name = f"Tổng {clean_label}"

    if sql_expression:
        metric_def = {
            "expressionType": "SQL",
            "sqlExpression": sql_expression,
            "hasCustomLabel": True,
            "label": clean_label,
        }
    else:
        metric_def = {
            "expressionType": "SIMPLE",
            "column": {
                "column_name": column_name,
                "type": "DOUBLE" if aggregate != "COUNT" else "BIGINT",
                "is_dttm": False,
                "filterable": True,
                "groupby": True,
            },
            "aggregate": aggregate,
            "hasCustomLabel": True,
            "label": clean_label,
        }

    chart_dict = {
        "slice_name": slice_name,
        "description": f"Chỉ số KPI {clean_label} ({table_name})",
        "viz_type": "big_number_total",
        "params": {
            "datasource": "1__table",
            "viz_type": "big_number_total",
            "adhoc_filters": adhoc_filters or [],
            "metric": metric_def,
            "header_font_size": 0.38,
            "subheader_font_size": 0.14,
            "y_axis_format": y_axis_format,
            "time_format": "smart_date",
        },
        "uuid": chart_uuid,
        "version": "1.0.0",
        "dataset_uuid": dataset_uuid,
    }
    return chart_dict, chart_uuid


def build_chart_pie(
    table_name: str,
    dataset_uuid: str,
    dimension_col: str,
    dimension_label: str,
    metric_col: str,
    metric_label: str,
    aggregate: str = "COUNT",
    number_format: str = ",.0f",
    uuid_suffix: str = "",
) -> Tuple[Dict[str, Any], str]:
    """Sinh Donut / Pie Chart phân tích cơ cấu tỷ trọng (Category Share)."""
    chart_uuid = get_deterministic_uuid(f"chart.pie.{table_name}.{dimension_col}{uuid_suffix}")
    slice_name = capitalize_abbreviations(f"Cơ cấu theo {dimension_label}")

    chart_dict = {
        "slice_name": slice_name,
        "description": f"Cơ cấu tỷ lệ theo {dimension_label} ({table_name})",
        "viz_type": "pie",
        "params": {
            "datasource": "1__table",
            "viz_type": "pie",
            "groupby": [dimension_col],
            "metric": {
                "expressionType": "SIMPLE",
                "column": {
                    "column_name": metric_col,
                    "type": "DOUBLE" if aggregate != "COUNT" else "BIGINT",
                    "is_dttm": False,
                    "filterable": True,
                    "groupby": True,
                },
                "aggregate": aggregate,
                "hasCustomLabel": True,
                "label": capitalize_abbreviations(metric_label),
            },
            "donut": True,
            "show_legend": True,
            "show_labels": True,
            "show_labels_threshold": 2,
            "label_type": "key_value",
            "number_format": number_format,
            "sort_by_metric": True,
            "row_limit": 10,
            "adhoc_filters": [],
        },
        "uuid": chart_uuid,
        "version": "1.0.0",
        "dataset_uuid": dataset_uuid,
    }
    return chart_dict, chart_uuid


def build_chart_bar(
    table_name: str,
    dataset_uuid: str,
    dimension_col: str,
    dimension_label: str,
    metric_col: str,
    metric_label: str,
    aggregate: str = "SUM",
    y_axis_format: str = ",.0f",
    is_top_10: bool = True,
) -> Tuple[Dict[str, Any], str]:
    """Sinh ECharts Bar Chart phân bổ theo Dimension (sắp xếp Top 10 rõ ràng, không đè chữ)."""
    chart_uuid = get_deterministic_uuid(f"chart.bar.{table_name}.{dimension_col}.{metric_col}")
    dim_clean = capitalize_abbreviations(dimension_label)
    met_clean = capitalize_abbreviations(metric_label)

    if is_top_10:
        clean_stat = met_clean.replace(" (Bình Quân)", "").replace("TB ", "")
        slice_name = f"Top 10 {dim_clean} có {clean_stat} cao nhất"
    else:
        slice_name = f"Phân bổ {met_clean} theo {dim_clean}"

    chart_dict = {
        "slice_name": slice_name,
        "description": f"Xếp hạng {dim_clean} theo {met_clean} ({table_name})",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "datasource": "1__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": dimension_col,
            "x_axis_title": dim_clean,
            "x_axis_sort_asc": False,
            "x_axis_sort_series": "value",
            "x_axis_sort_series_ascending": False,
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {
                        "column_name": metric_col,
                        "type": "DOUBLE" if aggregate != "COUNT" else "BIGINT",
                        "is_dttm": False,
                        "filterable": True,
                        "groupby": True,
                    },
                    "aggregate": aggregate,
                    "hasCustomLabel": True,
                    "label": met_clean,
                }
            ],
            "groupby": [],
            "seriesType": "bar",
            "color_scheme": "supersetColors",
            "show_legend": False,
            "show_value": True,
            "rich_tooltip": True,
            "row_limit": 10 if is_top_10 else 25,
            "truncateXAxis": False,
            "truncateYAxis": False,
            "y_axis_format": y_axis_format,
            "adhoc_filters": [],
        },
        "uuid": chart_uuid,
        "version": "1.0.0",
        "dataset_uuid": dataset_uuid,
    }
    return chart_dict, chart_uuid


def build_chart_table(
    table_name: str,
    dataset_uuid: str,
    all_columns: List[str],
    entity_label: str,
    metric_columns: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], str]:
    """Sinh Table Chart tra cứu chi tiết dữ liệu kèm cấu hình định dạng cột số."""
    chart_uuid = get_deterministic_uuid(f"chart.table.{table_name}")
    slice_name = capitalize_abbreviations(f"Chi tiết dữ liệu {entity_label}")

    column_config: Dict[str, Any] = {}
    if metric_columns:
        for m in metric_columns:
            m_snake = to_snake_case(m)
            is_avg = is_average_metric(m)
            if is_avg:
                column_config[f"avg_{m_snake}"] = {"d3NumberFormat": ",.2f"}
                column_config[f"sum_{m_snake}"] = {"d3NumberFormat": ",.2f"}
            else:
                column_config[f"sum_{m_snake}"] = {"d3NumberFormat": ",.0f"}
                column_config[f"avg_{m_snake}"] = {"d3NumberFormat": ",.1f"}

    chart_dict = {
        "slice_name": slice_name,
        "description": f"Bảng tra cứu chi tiết toàn bộ bản ghi cho {entity_label} ({table_name})",
        "viz_type": "table",
        "params": {
            "datasource": "1__table",
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": all_columns,
            "column_config": column_config,
            "include_search": True,
            "show_cell_bars": False,
            "server_page_length": 15,
            "row_limit": 1000,
            "order_by_cols": [],
            "adhoc_filters": [],
        },
        "uuid": chart_uuid,
        "version": "1.0.0",
        "dataset_uuid": dataset_uuid,
    }
    return chart_dict, chart_uuid


def build_chart_line(
    table_name: str,
    dataset_uuid: str,
    time_col: str,
    time_label: str,
    metric_col: str,
    metric_label: str,
    aggregate: str = "SUM",
    y_axis_format: str = ",.0f",
    groupby_cols: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], str]:
    """Sinh ECharts Time Series Line Chart xu hướng biến thiên theo thời gian."""
    chart_uuid = get_deterministic_uuid(f"chart.line.{table_name}.{time_col}.{metric_col}")
    met_clean = capitalize_abbreviations(metric_label)
    tim_clean = capitalize_abbreviations(time_label)
    slice_name = f"Xu hướng {met_clean} theo {tim_clean}"

    chart_dict = {
        "slice_name": slice_name,
        "description": f"Xu hướng {met_clean} theo thời gian ({table_name})",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "datasource": "1__table",
            "viz_type": "echarts_timeseries_line",
            "x_axis": time_col,
            "x_axis_title": tim_clean,
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {
                        "column_name": metric_col,
                        "type": "DOUBLE" if aggregate != "COUNT" else "BIGINT",
                        "is_dttm": False,
                        "filterable": True,
                        "groupby": True,
                    },
                    "aggregate": aggregate,
                    "hasCustomLabel": True,
                    "label": met_clean,
                }
            ],
            "groupby": groupby_cols or [],
            "seriesType": "line",
            "color_scheme": "supersetColors",
            "show_legend": bool(groupby_cols),
            "show_value": False,
            "rich_tooltip": True,
            "row_limit": 1000,
            "truncateXAxis": False,
            "truncateYAxis": False,
            "y_axis_format": y_axis_format,
            "adhoc_filters": [],
        },
        "uuid": chart_uuid,
        "version": "1.0.0",
        "dataset_uuid": dataset_uuid,
    }
    return chart_dict, chart_uuid


# =====================================================================
# DASHBOARD ARCHETYPE DETECTION & MULTI-LAYOUT GENERATORS
# =====================================================================

class DashboardArchetype(str, Enum):
    PERFORMANCE_RISK = "performance_risk"
    OPERATIONAL_PERFORMANCE = "operational_performance"
    TIME_SERIES = "time_series"
    ENTITY_CATALOG = "entity_catalog"
    CATEGORICAL_DISTRIBUTION = "categorical_distribution"


def detect_dashboard_archetype(
    decision: RoutingDecision,
    valid_cols: Set[str]
) -> DashboardArchetype:
    """Tự động nhận diện Archetype nghiệp vụ của Dataset để tuyển chọn Template phù hợp nhất."""
    explicit = (decision.dashboard_archetype or "").lower()
    if explicit in {item.value for item in DashboardArchetype}:
        return DashboardArchetype(explicit)

    domain_lower = (decision.dataset_domain or "").lower()
    entity_lower = (decision.dataset_entity or "").lower()

    # 1. IoT, Cảm biến, Giám sát thông số & Stream Telemetry -> TIME_SERIES (Tuyệt đối không dùng Pareto/Risk)
    iot_keywords = ["iot", "telemetry", "sensor", "environmental", "cam_bien", "device_log", "metric_stream", "scada"]
    if any(k in entity_lower for k in iot_keywords) or any(k in domain_lower for k in ["iot", "sensor", "telemetry"]):
        return DashboardArchetype.TIME_SERIES

    # 2. Khảo sát, Đánh giá sự hài lòng (Satisfaction, Survey, Feedback, Review, NPS, Ý kiến khách hàng) -> CATEGORICAL_DISTRIBUTION
    survey_keywords = ["satisfaction", "survey", "feedback", "rating", "review", "nps", "khao_sat", "danh_gia", "opinion", "poll"]
    if any(k in entity_lower for k in survey_keywords) or any(k in domain_lower for k in ["survey", "feedback", "satisfaction", "review"]):
        return DashboardArchetype.CATEGORICAL_DISTRIBUTION

    # 3. Nếu domain là KPI, đánh giá rủi ro, audit, SLA, benchmark -> PERFORMANCE_RISK (Pareto 80/20 & Health Status)
    if domain_lower in ["kpi", "risk", "evaluation", "benchmark", "sla", "audit"] or any(k in entity_lower for k in ["kpi", "muc_tieu", "danh_gia_hieu_suat"]):
        return DashboardArchetype.PERFORMANCE_RISK

    # 4. Nếu là danh mục thực thể (nhân vật game, sinh viên, sản phẩm, cán bộ, tài sản...) -> ENTITY_CATALOG
    entity_keywords = ["character", "student", "device", "asset", "product", "catalog", "master", "employee", "nhan_vat", "game", "gaming"]
    if any(k in entity_lower for k in entity_keywords) or domain_lower in ["gaming", "catalog", "master", "inventory"]:
        return DashboardArchetype.ENTITY_CATALOG

    # 5. Nếu có cột mốc thời gian thực sự hoặc source_updated_at_field và có metric đo lường -> TIME_SERIES
    # Điều kiện: Cột thời gian PHẢI là timestamp/date thực tế, KHÔNG được là metric số học (như departure_arrival_time_convenient)
    metric_cols_snake = {to_snake_case(m) for m in decision.metric_columns}
    strict_time_exact = {"time", "date", "ngay", "thang", "nam", "year", "month", "created_at", "updated_at", "timestamp", "datetime"}
    has_time = False
    if decision.source_updated_at_field and decision.source_updated_at_field.lower() in valid_cols:
        has_time = True
    else:
        for c in valid_cols:
            if is_technical_column(c) or c.startswith("_") or c in metric_cols_snake or f"sum_{c}" in valid_cols or f"avg_{c}" in valid_cols:
                continue
            if c in strict_time_exact or any(c.endswith(f"_{t}") for t in ["date", "time", "timestamp", "at", "dt"]):
                has_time = True
                break

    if has_time and len(decision.metric_columns) > 0 and domain_lower not in ["gaming", "catalog", "directory", "asset"]:
        return DashboardArchetype.TIME_SERIES

    # 6. Mặc định cho dữ liệu bảng/khảo sát/phân loại -> CATEGORICAL_DISTRIBUTION
    return DashboardArchetype.CATEGORICAL_DISTRIBUTION


# =====================================================================
# BUNDLE GENERATOR
# =====================================================================

def _build_layout_performance_risk(
    decision: RoutingDecision,
    dataset_uuid: str,
    valid_cols: Set[str],
    table_name: str,
    entity_label: str,
    primary_eval_metric: Optional[str],
    rank_metric: Optional[str],
    _resolve,
) -> Tuple[List[Tuple[Dict[str, Any], str]], Dict[str, Any], str, str, Dict[str, str]]:
    """Layout 1: Hiệu suất & Quản trị Rủi ro (5 Tầng: Domain KPIs -> Visuals -> Decision KPIs -> Pareto/Health -> Table)."""
    charts: List[Tuple[Dict[str, Any], str]] = []

    # =================================================================
    # PHẦN 1 (TRÊN CÙNG): CÁC CHỈ SỐ VÀ BIỂU ĐỒ DỮ LIỆU THÔNG THƯỜNG
    # =================================================================
    # HÀNG 1: DOMAIN KPI CARDS (4 Cards, Width 3 each)
    # 1. Tổng số bản ghi (total_records)
    kpi_records, kpi_rec_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="total_records_count",
        metric_label="Tổng Số Bản Ghi",
        column_name=_resolve("total_records"),
        aggregate="SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_records, kpi_rec_uuid))

    # 2. Tổng Chỉ Số Nghiệp Vụ Chính
    if primary_eval_metric:
        p_snake = to_snake_case(primary_eval_metric)
        p_label = to_vietnamese_label(primary_eval_metric)
        p_field = _resolve(f"sum_{p_snake}", fallback=_resolve(p_snake, "total_records"))
        kpi_outcome, kpi_out_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="primary_outcome",
            metric_label=f"Tổng {p_label}" if p_field.startswith("sum_") else f"{p_label}",
            column_name=p_field,
            aggregate="SUM",
            y_axis_format=",.0f",
        )
    else:
        kpi_outcome, kpi_out_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="primary_outcome",
            metric_label="Quy Mô Bản Ghi",
            column_name=_resolve("total_records"),
            aggregate="SUM",
            y_axis_format=",.0f",
        )
    charts.append((kpi_outcome, kpi_out_uuid))

    # 3. TB Chỉ Số Nghiệp Vụ Chính
    if primary_eval_metric:
        p_snake = to_snake_case(primary_eval_metric)
        p_label = to_vietnamese_label(primary_eval_metric)
        is_avg = is_average_metric(primary_eval_metric)
        p_avg_field = _resolve(f"avg_{p_snake}", fallback=_resolve(p_snake, "total_records"))
        kpi_average, kpi_avg_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="primary_average",
            metric_label=f"{p_label} (Bình Quân)" if is_avg else f"TB {p_label}",
            column_name=p_avg_field,
            aggregate="AVG" if p_avg_field.startswith("avg_") else "SUM",
            y_axis_format=",.2f" if any(k in p_snake for k in ["tb", "gpa", "rate", "diem"]) else ",.1f",
        )
    else:
        kpi_average, kpi_avg_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="primary_average",
            metric_label="Hiệu Suất Dữ Liệu",
            column_name=_resolve("total_records"),
            aggregate="SUM",
            y_axis_format=",.0f",
        )
    charts.append((kpi_average, kpi_avg_uuid))

    # 4. Chỉ Số Bổ Trợ: Tỷ Lệ Hoàn Thành (%) hoặc Metric Thứ 2
    if "ty_le_hoan_thanh_phan_tram" in valid_cols:
        kpi_secondary, kpi_sec_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="avg_completion_pct",
            metric_label="Tỷ Lệ Hoàn Thành (Bình Quân)",
            column_name="ty_le_hoan_thanh_phan_tram",
            aggregate="AVG",
            y_axis_format=",.1f",
        )
    elif rank_metric and rank_metric != primary_eval_metric:
        r_snake = to_snake_case(rank_metric)
        r_label = to_vietnamese_label(rank_metric)
        r_is_avg = is_average_metric(rank_metric)
        r_target = f"avg_{r_snake}" if r_is_avg else f"sum_{r_snake}"
        r_field_col = _resolve(r_target, fallback=_resolve(r_snake, "total_records"))
        kpi_secondary, kpi_sec_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="secondary_metric",
            metric_label=f"{r_label} (Bình Quân)" if r_is_avg else f"Tổng {r_label}",
            column_name=r_field_col,
            aggregate="AVG" if r_is_avg else "SUM",
            y_axis_format=",.2f" if any(k in r_snake for k in ["tb", "gpa", "rate"]) else ",.0f",
        )
    else:
        kpi_secondary, kpi_sec_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="secondary_metric",
            metric_label="Chỉ Số Quy Mô",
            column_name=_resolve("total_records"),
            aggregate="SUM",
            y_axis_format=",.0f",
        )
    charts.append((kpi_secondary, kpi_sec_uuid))

    # HÀNG 2: DOMAIN DATA VISUALS (2 Charts, Width 6 each)
    donut_keywords = [
        "rarity", "xep_loai", "trang_thai", "element", "gioi_tinh", "hoc_ky",
        "faculty", "khoa", "don_vi", "category", "tier", "status", "type", "roles", "role"
    ]
    entity_keywords = [
        "name", "ho_ten", "student_name", "full_name", "mssv", "ma_sv", "id", "title"
    ]
    donut_candidates = [d for d in decision.dimension_columns if any(k in d.lower() for k in donut_keywords)]
    entity_candidates = [d for d in decision.dimension_columns if any(k in d.lower() for k in entity_keywords)]
    if not donut_candidates and decision.dimension_columns:
        donut_candidates.append(decision.dimension_columns[0])
    if not entity_candidates and decision.dimension_columns:
        entity_candidates.append(decision.dimension_columns[0])

    cat_dim = donut_candidates[0] if donut_candidates else "nhom_don_vi"
    cat_snake = to_snake_case(cat_dim)
    ent_dim = entity_candidates[0] if entity_candidates else "nhom_don_vi"
    ent_snake = to_snake_case(ent_dim)

    # 1. Donut Category Breakdown (Cơ cấu theo danh mục thực tế)
    cat_dim_resolved = _resolve(cat_snake)
    pie_domain_cat, pie_dom_uuid = build_chart_pie(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=cat_dim_resolved,
        dimension_label=to_vietnamese_label(cat_dim_resolved),
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        number_format=",.0f",
    )
    charts.append((pie_domain_cat, pie_dom_uuid))

    # 2. Top 10 Leaderboard (Bảng xếp hạng dẫn đầu theo số liệu thực tế)
    ent_dim_resolved = _resolve(ent_snake)
    r_field = f"avg_{to_snake_case(rank_metric)}" if (rank_metric and is_average_metric(rank_metric)) else (f"sum_{to_snake_case(rank_metric)}" if rank_metric else "total_records")
    r_field_resolved = _resolve(r_field, fallback=_resolve("total_records"))
    r_lbl = to_vietnamese_label(rank_metric) if rank_metric else "Số lượng"
    r_agg = "AVG" if (rank_metric and is_average_metric(rank_metric) and r_field_resolved.startswith("avg_")) else "SUM"
    bar_domain_rank, bar_dom_uuid = build_chart_bar(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=ent_dim_resolved,
        dimension_label=to_vietnamese_label(ent_dim_resolved),
        metric_col=r_field_resolved,
        metric_label=r_lbl,
        aggregate=r_agg,
        y_axis_format=",.0f",
        is_top_10=True,
    )
    charts.append((bar_domain_rank, bar_dom_uuid))

    # =================================================================
    # PHẦN 2 (DỜI XUỐNG DƯỚI): RA QUYẾT ĐỊNH & CẢNH BÁO NGUY CƠ
    # =================================================================
    # HÀNG 3: DECISION KPI CARDS (4 Cards, Width 3 each)
    # 1. Số ca cần can thiệp khẩn (CRITICAL)
    if "health_status" in valid_cols:
        kpi_risk, kpi_risk_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="risk_critical_count",
            metric_label="Cần Can Thiệp Khẩn (CRITICAL)",
            sql_expression="COUNT(CASE WHEN health_status = 'CRITICAL' THEN 1 END)",
            y_axis_format=",.0f",
        )
    else:
        kpi_risk, kpi_risk_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="risk_critical_count",
            metric_label="Cần Can Thiệp Khẩn (CRITICAL)",
            column_name=_resolve("total_records"),
            aggregate="SUM",
            y_axis_format=",.0f",
        )
    charts.append((kpi_risk, kpi_risk_uuid))

    # 2. Tỷ Lệ Đạt Mục Tiêu Bình Quân
    achieve_target = "target_achievement_pct" if "target_achievement_pct" in valid_cols else _resolve("total_records")
    kpi_achieve, kpi_achieve_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="avg_target_achievement",
        metric_label="Tỷ Lệ Đạt Mục Tiêu (Bình Quân)" if achieve_target == "target_achievement_pct" else "Tổng Số Bản Ghi",
        column_name=achieve_target,
        aggregate="AVG" if achieve_target == "target_achievement_pct" else "SUM",
        y_axis_format=",.1f" if achieve_target == "target_achievement_pct" else ",.0f",
    )
    charts.append((kpi_achieve, kpi_achieve_uuid))

    # 3. Mục Tiêu Định Mức Benchmark
    benchmark_target = "target_value" if "target_value" in valid_cols else _resolve("total_records")
    kpi_benchmark, kpi_bench_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="target_benchmark_val",
        metric_label="Mục Tiêu Định Mức Benchmark" if benchmark_target == "target_value" else "Số Bản Ghi Định Mức",
        column_name=benchmark_target,
        aggregate="AVG" if benchmark_target == "target_value" else "SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_benchmark, kpi_bench_uuid))

    # 4. Điểm Đánh Giá Rủi Ro Bình Quân
    risk_target = "risk_score" if "risk_score" in valid_cols else _resolve("total_records")
    kpi_risk_score, kpi_rscore_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="avg_risk_score",
        metric_label="Điểm Đánh Giá Rủi Ro (Bình Quân)" if risk_target == "risk_score" else "Mức Rủi Ro Chung",
        column_name=risk_target,
        aggregate="AVG" if risk_target == "risk_score" else "SUM",
        y_axis_format=",.2f" if risk_target == "risk_score" else ",.0f",
    )
    charts.append((kpi_risk_score, kpi_rscore_uuid))

    # HÀNG 4: DECISION VISUALS (2 Charts, Width 6 each)
    # 1. Donut Chart - Trạng thái sức khỏe nghiệp vụ & Cảnh báo nguy cơ
    health_dim = _resolve("health_status", fallback=cat_snake)
    pie_health, pie_health_uuid = build_chart_pie(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=health_dim,
        dimension_label="Trạng Thái Sức Khỏe & Nguy Cơ" if health_dim == "health_status" else to_vietnamese_label(health_dim),
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        number_format=",.0f",
    )
    charts.append((pie_health, pie_health_uuid))

    # 2. Bar Chart - Phân bổ Hiệu suất theo Phân khúc Pareto (80/20)
    tier_dim = _resolve("performance_tier", fallback=ent_snake)
    p_metric_field = _resolve(f"sum_{to_snake_case(primary_eval_metric)}" if primary_eval_metric else "total_records")
    p_metric_label = f"Tổng {to_vietnamese_label(primary_eval_metric)}" if primary_eval_metric else "Số lượng"
    bar_tier, bar_tier_uuid = build_chart_bar(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=tier_dim,
        dimension_label="Phân Khúc Hiệu Suất Pareto" if tier_dim == "performance_tier" else to_vietnamese_label(tier_dim),
        metric_col=p_metric_field,
        metric_label=p_metric_label,
        aggregate="SUM",
        y_axis_format=",.0f",
        is_top_10=False,
    )
    charts.append((bar_tier, bar_tier_uuid))

    # =================================================================
    # PHẦN 3 (CUỐI CÙNG): BẢNG DỮ LIỆU TÁC NGHIỆP CHI TIẾT
    # =================================================================
    all_col_names = [c for c in valid_cols if not c.startswith("_")]
    priority_front_cols = []
    # 1. Entity identifier
    for d in entity_candidates:
        ds = to_snake_case(d)
        if ds in all_col_names and ds not in priority_front_cols:
            priority_front_cols.append(ds)
    # 2. Dimensions chính
    for d in decision.dimension_columns:
        ds = to_snake_case(d)
        if ds in all_col_names and ds not in priority_front_cols and len(priority_front_cols) < 5:
            priority_front_cols.append(ds)
    # 3. Metrics thực tế
    for m in [primary_eval_metric, rank_metric]:
        if m:
            ms = to_snake_case(m)
            if f"sum_{ms}" in all_col_names and f"sum_{ms}" not in priority_front_cols:
                priority_front_cols.append(f"sum_{ms}")
            if f"avg_{ms}" in all_col_names and f"avg_{ms}" not in priority_front_cols:
                priority_front_cols.append(f"avg_{ms}")
    # 4. Cột tỷ lệ hoàn thành hoặc nghiệp vụ
    if "ty_le_hoan_thanh_phan_tram" in all_col_names and "ty_le_hoan_thanh_phan_tram" not in priority_front_cols:
        priority_front_cols.append("ty_le_hoan_thanh_phan_tram")
    # 5. Decision fields bắt buộc
    for df in ["target_achievement_pct", "health_status", "action_priority", "recommended_action"]:
        if df in all_col_names and df not in priority_front_cols:
            priority_front_cols.append(df)

    priority_front_cols = [c for c in priority_front_cols if c.lower() in valid_cols]
    if not priority_front_cols:
        priority_front_cols = list(valid_cols)[:8]

    table_chart, table_uuid = build_chart_table(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        all_columns=priority_front_cols,
        entity_label="Tác Nghiệp & Khuyến Nghị Hành Động",
        metric_columns=decision.metric_columns,
    )
    table_chart["slice_name"] = "Danh Sách Tác Nghiệp & Khuyến Nghị Hành Động"
    charts.append((table_chart, table_uuid))

    # 4. Dashboard Layout Grid (Mô hình 5 tầng: Thông thường ở trên, Decision ở dưới, Bảng cuối cùng)
    position: Dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "children": ["GRID_ID"], "id": "ROOT_ID"},
        "GRID_ID": {
            "type": "GRID",
            "children": [
                "ROW-DOMAIN-KPI-1",
                "ROW-DOMAIN-CHARTS-2",
                "ROW-DECISION-KPI-3",
                "ROW-DECISION-CHARTS-4",
                "ROW-OPERATIONAL-5",
            ],
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
        },
    }

    # BƯỚC 1: HÀNG 1 - CÁC CARD SỐ THÔNG THƯỜNG (4 Cards, Width 3 each)
    kpi_domain_children = ["CHART-DOMAIN-KPI-1", "CHART-DOMAIN-KPI-2", "CHART-DOMAIN-KPI-3", "CHART-DOMAIN-KPI-4"]
    for idx, chart_tuple in enumerate([(kpi_records, kpi_rec_uuid), (kpi_outcome, kpi_out_uuid), (kpi_average, kpi_avg_uuid), (kpi_secondary, kpi_sec_uuid)]):
        c_data, c_uuid = chart_tuple
        c_key = f"CHART-DOMAIN-KPI-{idx + 1}"
        position[c_key] = {
            "children": [],
            "id": c_key,
            "meta": {
                "chartId": idx + 1,
                "height": 22,
                "sliceName": c_data["slice_name"],
                "uuid": c_uuid,
                "width": 3,
            },
            "parents": ["ROOT_ID", "GRID_ID", "ROW-DOMAIN-KPI-1"],
            "type": "CHART",
        }
    position["ROW-DOMAIN-KPI-1"] = {
        "children": kpi_domain_children,
        "id": "ROW-DOMAIN-KPI-1",
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"],
        "type": "ROW",
    }

    # BƯỚC 2: HÀNG 2 - BIỂU ĐỒ PHÂN TÍCH DỮ LIỆU THỰC TẾ (Width 6 & 6)
    position["CHART-DOMAIN-VISUAL-1"] = {
        "children": [],
        "id": "CHART-DOMAIN-VISUAL-1",
        "meta": {
            "chartId": 11,
            "height": 48,
            "sliceName": pie_domain_cat["slice_name"],
            "uuid": pie_dom_uuid,
            "width": 6,
        },
        "parents": ["ROOT_ID", "GRID_ID", "ROW-DOMAIN-CHARTS-2"],
        "type": "CHART",
    }
    position["CHART-DOMAIN-VISUAL-2"] = {
        "children": [],
        "id": "CHART-DOMAIN-VISUAL-2",
        "meta": {
            "chartId": 12,
            "height": 48,
            "sliceName": bar_domain_rank["slice_name"],
            "uuid": bar_dom_uuid,
            "width": 6,
        },
        "parents": ["ROOT_ID", "GRID_ID", "ROW-DOMAIN-CHARTS-2"],
        "type": "CHART",
    }
    position["ROW-DOMAIN-CHARTS-2"] = {
        "children": ["CHART-DOMAIN-VISUAL-1", "CHART-DOMAIN-VISUAL-2"],
        "id": "ROW-DOMAIN-CHARTS-2",
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"],
        "type": "ROW",
    }

    # BƯỚC 3: HÀNG 3 - CÁC CARD SỐ RA QUYẾT ĐỊNH & CẢNH BÁO (4 Cards, Width 3 each)
    kpi_decision_children = ["CHART-DECISION-KPI-1", "CHART-DECISION-KPI-2", "CHART-DECISION-KPI-3", "CHART-DECISION-KPI-4"]
    for idx, chart_tuple in enumerate([(kpi_risk, kpi_risk_uuid), (kpi_achieve, kpi_achieve_uuid), (kpi_benchmark, kpi_bench_uuid), (kpi_risk_score, kpi_rscore_uuid)]):
        c_data, c_uuid = chart_tuple
        c_key = f"CHART-DECISION-KPI-{idx + 1}"
        position[c_key] = {
            "children": [],
            "id": c_key,
            "meta": {
                "chartId": 21 + idx,
                "height": 22,
                "sliceName": c_data["slice_name"],
                "uuid": c_uuid,
                "width": 3,
            },
            "parents": ["ROOT_ID", "GRID_ID", "ROW-DECISION-KPI-3"],
            "type": "CHART",
        }
    position["ROW-DECISION-KPI-3"] = {
        "children": kpi_decision_children,
        "id": "ROW-DECISION-KPI-3",
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"],
        "type": "ROW",
    }

    # BƯỚC 4: HÀNG 4 - BIỂU ĐỒ RA QUYẾT ĐỊNH & PHÂN KHÚC (Width 6 & 6)
    position["CHART-DECISION-VISUAL-1"] = {
        "children": [],
        "id": "CHART-DECISION-VISUAL-1",
        "meta": {
            "chartId": 31,
            "height": 48,
            "sliceName": pie_health["slice_name"],
            "uuid": pie_health_uuid,
            "width": 6,
        },
        "parents": ["ROOT_ID", "GRID_ID", "ROW-DECISION-CHARTS-4"],
        "type": "CHART",
    }
    position["CHART-DECISION-VISUAL-2"] = {
        "children": [],
        "id": "CHART-DECISION-VISUAL-2",
        "meta": {
            "chartId": 32,
            "height": 48,
            "sliceName": bar_tier["slice_name"],
            "uuid": bar_tier_uuid,
            "width": 6,
        },
        "parents": ["ROOT_ID", "GRID_ID", "ROW-DECISION-CHARTS-4"],
        "type": "CHART",
    }
    position["ROW-DECISION-CHARTS-4"] = {
        "children": ["CHART-DECISION-VISUAL-1", "CHART-DECISION-VISUAL-2"],
        "id": "ROW-DECISION-CHARTS-4",
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"],
        "type": "ROW",
    }

    # BƯỚC 5: HÀNG 5 - BẢNG DỮ LIỆU TÁC NGHIỆP CHI TIẾT (Width 12 - LUÔN Ở CUỐI CÙNG)
    position["CHART-OPERATIONAL-TABLE"] = {
        "children": [],
        "id": "CHART-OPERATIONAL-TABLE",
        "meta": {
            "chartId": 99,
            "height": 60,
            "sliceName": table_chart["slice_name"],
            "uuid": table_uuid,
            "width": 12,
        },
        "parents": ["ROOT_ID", "GRID_ID", "ROW-OPERATIONAL-5"],
        "type": "CHART",
    }
    position["ROW-OPERATIONAL-5"] = {
        "children": ["CHART-OPERATIONAL-TABLE"],
        "id": "ROW-OPERATIONAL-5",
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"],
        "type": "ROW",
    }

    dashboard_title = f"[Auto] {entity_label} Decision Analytics"
    description = f"AI Decision-Driven Dashboard cho {entity_label}. Tự động sinh theo mô hình 5 tầng (Domain KPIs -> Visuals -> Decision KPIs -> Pareto/Health -> Action Table)."

    decision_label_colors = {
        "CRITICAL": "#e04141",
        "AT_RISK": "#f0ad4e",
        "HEALTHY": "#5cb85c",
        "HIGH": "#e04141",
        "MEDIUM": "#f0ad4e",
        "LOW": "#5cb85c",
        "TIER_A": "#337ab7",
        "TIER_B": "#5bc0de",
        "TIER_C": "#999999",
    }
    return charts, position, dashboard_title, description, decision_label_colors


def _build_layout_time_series(
    decision: RoutingDecision,
    dataset_uuid: str,
    valid_cols: Set[str],
    table_name: str,
    entity_label: str,
    primary_eval_metric: Optional[str],
    rank_metric: Optional[str],
    _resolve,
) -> Tuple[List[Tuple[Dict[str, Any], str]], Dict[str, Any], str, str, Dict[str, str]]:
    """Layout 2: Chuỗi thời gian & Xu hướng (4 Tầng: Trend KPIs -> Full-width Line -> Comparison Visuals -> Table)."""
    charts: List[Tuple[Dict[str, Any], str]] = []

    time_keywords = ["timestamp", "datetime", "created_at", "updated_at", "date", "time", "ngay"]
    detected_time = None
    metric_cols_snake = {to_snake_case(m) for m in decision.metric_columns}
    if decision.source_updated_at_field and decision.source_updated_at_field.lower() in valid_cols:
        detected_time = decision.source_updated_at_field
    else:
        for c in valid_cols:
            if c.startswith("_") or c in metric_cols_snake or f"sum_{c}" in valid_cols or f"avg_{c}" in valid_cols:
                continue
            if c in time_keywords or any(c.endswith(f"_{t}") for t in ["date", "time", "timestamp", "at", "dt"]):
                detected_time = c
                break
    time_col = detected_time or _resolve("thoi_gian_cap_nhat", fallback=list(valid_cols)[0])
    time_label = to_vietnamese_label(time_col)

    p_snake = to_snake_case(primary_eval_metric) if primary_eval_metric else "total_records"
    p_label = to_vietnamese_label(primary_eval_metric) if primary_eval_metric else "Bản Ghi"
    is_p_avg = is_average_metric(primary_eval_metric) if primary_eval_metric else False
    p_sum_field = _resolve(f"sum_{p_snake}", fallback=_resolve(p_snake, "total_records"))
    p_avg_field = _resolve(f"avg_{p_snake}", fallback=_resolve(p_snake, "total_records"))
    p_metric_field = p_avg_field if is_p_avg else p_sum_field
    p_metric_agg = "AVG" if is_p_avg else "SUM"
    p_metric_format = ",.1f" if is_p_avg else ",.0f"

    cat_dim = decision.dimension_columns[0] if decision.dimension_columns else "nhom_don_vi"
    cat_dim_resolved = _resolve(to_snake_case(cat_dim))
    cat_label = to_vietnamese_label(cat_dim_resolved)

    # Ưu tiên chiều phân loại và chiều định danh thực thể/thiết bị
    device_dims = [d for d in decision.dimension_columns if any(k in d.lower() for k in ["device", "thiet_bi", "sensor", "node", "id", "name", "station"])]
    if device_dims:
        ent_dim = device_dims[0]
    else:
        ent_dim = decision.dimension_columns[-1] if len(decision.dimension_columns) > 1 else cat_dim
    ent_dim_resolved = _resolve(to_snake_case(ent_dim))
    ent_label = to_vietnamese_label(ent_dim_resolved)

    # HÀNG 1: TREND KPIS (4 Cards, Width 3 each)
    kpi_primary, kpi_pri_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="ts_primary_scale",
        metric_label=f"TB {p_label}" if is_p_avg else (f"Tổng {p_label}" if p_sum_field.startswith("sum_") else f"Quy Mô {p_label}"),
        column_name=p_metric_field,
        aggregate=p_metric_agg,
        y_axis_format=p_metric_format,
    )
    charts.append((kpi_primary, kpi_pri_uuid))

    kpi_peak, kpi_peak_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="ts_peak_value",
        metric_label=f"Đỉnh Điểm {p_label}",
        column_name=p_metric_field,
        aggregate="MAX",
        y_axis_format=p_metric_format,
    )
    charts.append((kpi_peak, kpi_peak_uuid))

    kpi_cnt, kpi_cnt_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="ts_total_events",
        metric_label="Tổng Số Lượt Ghi Nhận",
        column_name=_resolve("total_records"),
        aggregate="SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_cnt, kpi_cnt_uuid))

    if rank_metric and rank_metric != primary_eval_metric:
        r_snake = to_snake_case(rank_metric)
        r_label = to_vietnamese_label(rank_metric)
        is_r_avg = is_average_metric(rank_metric)
        r_field = _resolve(f"avg_{r_snake}" if is_r_avg else f"sum_{r_snake}", fallback=_resolve(r_snake, "total_records"))
        kpi_secondary, kpi_sec_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="ts_secondary_metric",
            metric_label=f"TB {r_label}" if is_r_avg else f"Tổng {r_label}",
            column_name=r_field,
            aggregate="AVG" if is_r_avg else "SUM",
            y_axis_format=",.1f" if is_r_avg else ",.0f",
        )
    else:
        kpi_secondary, kpi_sec_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="ts_avg_period",
            metric_label=f"TB {p_label} Định Kỳ",
            column_name=p_avg_field,
            aggregate="AVG" if p_avg_field.startswith("avg_") else "SUM",
            y_axis_format=",.1f" if is_p_avg else ",.0f",
        )
    charts.append((kpi_secondary, kpi_sec_uuid))

    # HÀNG 2: FULL-WIDTH TIME SERIES LINE (Width 12)
    line_chart, line_uuid = build_chart_line(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        time_col=time_col,
        time_label=time_label,
        metric_col=p_metric_field,
        metric_label=p_label,
        aggregate=p_metric_agg,
        y_axis_format=p_metric_format,
        groupby_cols=[cat_dim_resolved] if cat_dim_resolved != time_col else [],
    )
    charts.append((line_chart, line_uuid))

    # HÀNG 3: COMPARISON VISUALS (Width 6 & 6)
    pie_cat, pie_uuid = build_chart_pie(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=cat_dim_resolved,
        dimension_label=cat_label,
        metric_col=_resolve("total_records"),
        metric_label="Số Lượng Bản Ghi",
        aggregate="SUM",
        number_format=",.0f",
    )
    pie_cat["slice_name"] = capitalize_abbreviations(f"Cơ Cấu Bản Ghi Theo {cat_label}")
    charts.append((pie_cat, pie_uuid))

    bar_rank, bar_uuid = build_chart_bar(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=ent_dim_resolved,
        dimension_label=ent_label,
        metric_col=p_metric_field,
        metric_label=p_label,
        aggregate=p_metric_agg,
        y_axis_format=p_metric_format,
        is_top_10=True,
    )
    bar_rank["slice_name"] = capitalize_abbreviations(f"So Sánh {p_label} Theo {ent_label}")
    charts.append((bar_rank, bar_uuid))

    # HÀNG 4: BẢNG LỊCH SỬ DỮ LIỆU (Width 12 - Ở cuối cùng)
    all_cols = [c for c in valid_cols if not c.startswith("_")]
    table_chart, table_uuid = build_chart_table(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        all_columns=all_cols,
        entity_label=entity_label,
        metric_columns=decision.metric_columns,
    )
    table_chart["slice_name"] = f"Lịch Sử Dữ Liệu {entity_label} Theo Mốc Thời Gian"
    charts.append((table_chart, table_uuid))

    # Grid Position
    position: Dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "children": ["GRID_ID"], "id": "ROOT_ID"},
        "GRID_ID": {
            "type": "GRID",
            "children": [
                "ROW-TREND-KPI-1",
                "ROW-TREND-LINE-2",
                "ROW-TREND-CHARTS-3",
                "ROW-OPERATIONAL-4",
            ],
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
        },
    }
    kpi_children = ["CHART-TREND-KPI-1", "CHART-TREND-KPI-2", "CHART-TREND-KPI-3", "CHART-TREND-KPI-4"]
    for idx, c_tuple in enumerate([(kpi_primary, kpi_pri_uuid), (kpi_peak, kpi_peak_uuid), (kpi_cnt, kpi_cnt_uuid), (kpi_secondary, kpi_sec_uuid)]):
        c_data, c_uuid = c_tuple
        k_id = f"CHART-TREND-KPI-{idx + 1}"
        position[k_id] = {
            "children": [], "id": k_id,
            "meta": {"chartId": 101 + idx, "height": 22, "sliceName": c_data["slice_name"], "uuid": c_uuid, "width": 3},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-TREND-KPI-1"], "type": "CHART"
        }
    position["ROW-TREND-KPI-1"] = {
        "children": kpi_children, "id": "ROW-TREND-KPI-1", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-TREND-LINE-1"] = {
        "children": [], "id": "CHART-TREND-LINE-1",
        "meta": {"chartId": 111, "height": 50, "sliceName": line_chart["slice_name"], "uuid": line_uuid, "width": 12},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-TREND-LINE-2"], "type": "CHART"
    }
    position["ROW-TREND-LINE-2"] = {
        "children": ["CHART-TREND-LINE-1"], "id": "ROW-TREND-LINE-2", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-TREND-VISUAL-1"] = {
        "children": [], "id": "CHART-TREND-VISUAL-1",
        "meta": {"chartId": 121, "height": 48, "sliceName": pie_cat["slice_name"], "uuid": pie_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-TREND-CHARTS-3"], "type": "CHART"
    }
    position["CHART-TREND-VISUAL-2"] = {
        "children": [], "id": "CHART-TREND-VISUAL-2",
        "meta": {"chartId": 122, "height": 48, "sliceName": bar_rank["slice_name"], "uuid": bar_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-TREND-CHARTS-3"], "type": "CHART"
    }
    position["ROW-TREND-CHARTS-3"] = {
        "children": ["CHART-TREND-VISUAL-1", "CHART-TREND-VISUAL-2"], "id": "ROW-TREND-CHARTS-3", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-OPERATIONAL-TABLE"] = {
        "children": [], "id": "CHART-OPERATIONAL-TABLE",
        "meta": {"chartId": 199, "height": 60, "sliceName": table_chart["slice_name"], "uuid": table_uuid, "width": 12},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-OPERATIONAL-4"], "type": "CHART"
    }
    position["ROW-OPERATIONAL-4"] = {
        "children": ["CHART-OPERATIONAL-TABLE"], "id": "ROW-OPERATIONAL-4", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }

    title = f"[Auto] {entity_label} Time Series & Trend Analytics"
    desc = f"Dashboard phân tích chuỗi thời gian và xu hướng tăng trưởng cho {entity_label}."
    return charts, position, title, desc, {}


def _build_layout_entity_catalog(
    decision: RoutingDecision,
    dataset_uuid: str,
    valid_cols: Set[str],
    table_name: str,
    entity_label: str,
    primary_eval_metric: Optional[str],
    rank_metric: Optional[str],
    _resolve,
) -> Tuple[List[Tuple[Dict[str, Any], str]], Dict[str, Any], str, str, Dict[str, str]]:
    """Layout 3: Danh mục & Hồ sơ Thực thể (4 Tầng: Overview KPIs -> Core Attributes -> Secondary Attributes -> Table)."""
    charts: List[Tuple[Dict[str, Any], str]] = []

    p_snake = to_snake_case(primary_eval_metric) if primary_eval_metric else "total_records"
    p_label = to_vietnamese_label(primary_eval_metric) if primary_eval_metric else "Chỉ Số Cốt Lõi"
    p_sum_field = _resolve(f"sum_{p_snake}", fallback=_resolve(p_snake, "total_records"))
    p_avg_field = _resolve(f"avg_{p_snake}", fallback=_resolve(p_snake, "total_records"))

    r_snake = to_snake_case(rank_metric) if (rank_metric and rank_metric != primary_eval_metric) else None
    r_label = to_vietnamese_label(rank_metric) if r_snake else "Chỉ Số Thứ Cấp"
    r_avg_field = _resolve(f"avg_{r_snake}", fallback=_resolve(r_snake, "total_records")) if r_snake else p_avg_field

    cat_keywords = ["rarity", "element", "faculty", "khoa", "don_vi", "type", "role", "roles", "category", "xep_loai", "tier"]
    name_keywords = ["name", "ho_ten", "student_name", "title", "character_name", "ten", "id", "mssv"]

    cat_candidates = [d for d in decision.dimension_columns if any(k in d.lower() for k in cat_keywords)]
    name_candidates = [d for d in decision.dimension_columns if any(k in d.lower() for k in name_keywords)]

    cat_dim = cat_candidates[0] if cat_candidates else (decision.dimension_columns[0] if decision.dimension_columns else "loai")
    ent_dim = name_candidates[0] if name_candidates else (decision.dimension_columns[-1] if decision.dimension_columns else "ten")
    remaining_dims = [d for d in decision.dimension_columns if d != cat_dim]
    sub_cat_dim = cat_candidates[1] if len(cat_candidates) > 1 else (remaining_dims[0] if remaining_dims else cat_dim)

    cat_dim_res = _resolve(to_snake_case(cat_dim))
    ent_dim_res = _resolve(to_snake_case(ent_dim))
    sub_cat_res = _resolve(to_snake_case(sub_cat_dim))

    # HÀNG 1: CATALOG OVERVIEW KPIS (4 Cards, Width 3 each)
    kpi_cnt, kpi_cnt_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="cat_total_entities",
        metric_label="Tổng Số Thực Thể",
        column_name=_resolve("total_records"),
        aggregate="SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_cnt, kpi_cnt_uuid))

    kpi_p_avg, kpi_p_avg_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="cat_primary_avg",
        metric_label=f"TB {p_label}",
        column_name=p_avg_field,
        aggregate="AVG" if p_avg_field.startswith("avg_") else "SUM",
        y_axis_format=",.1f",
    )
    charts.append((kpi_p_avg, kpi_p_avg_uuid))

    kpi_p_max, kpi_p_max_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="cat_primary_max",
        metric_label=f"{p_label} Đỉnh Điểm (MAX)",
        column_name=p_sum_field,
        aggregate="MAX" if not p_sum_field.startswith("sum_") else "SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_p_max, kpi_p_max_uuid))

    kpi_r_avg, kpi_r_avg_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="cat_secondary_avg",
        metric_label=f"TB {r_label}" if r_snake else f"Quy Mô {p_label}",
        column_name=r_avg_field,
        aggregate="AVG" if r_avg_field.startswith("avg_") else "SUM",
        y_axis_format=",.1f",
    )
    charts.append((kpi_r_avg, kpi_r_avg_uuid))

    # HÀNG 2: CORE ATTRIBUTE VISUALS (Width 6 & 6)
    pie_cat, pie_uuid = build_chart_pie(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=cat_dim_res,
        dimension_label=to_vietnamese_label(cat_dim_res),
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        number_format=",.0f",
    )
    charts.append((pie_cat, pie_uuid))

    bar_rank, bar_uuid = build_chart_bar(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=ent_dim_res,
        dimension_label=to_vietnamese_label(ent_dim_res),
        metric_col=p_sum_field,
        metric_label=p_label,
        aggregate="SUM",
        y_axis_format=",.0f",
        is_top_10=True,
    )
    charts.append((bar_rank, bar_uuid))

    # HÀNG 3: SECONDARY ATTRIBUTE VISUALS (Width 6 & 6)
    bar_cat_stat, bar_cstat_uuid = build_chart_bar(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=cat_dim_res,
        dimension_label=to_vietnamese_label(cat_dim_res),
        metric_col=p_avg_field,
        metric_label=f"{p_label} (Bình Quân)",
        aggregate="AVG" if p_avg_field.startswith("avg_") else "SUM",
        y_axis_format=",.1f",
        is_top_10=False,
    )
    charts.append((bar_cat_stat, bar_cstat_uuid))

    pie_sub, pie_sub_uuid = build_chart_pie(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=sub_cat_res,
        dimension_label=to_vietnamese_label(sub_cat_res),
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        number_format=",.0f",
        uuid_suffix=".sub" if sub_cat_res == cat_dim_res else "",
    )
    if sub_cat_res == cat_dim_res:
        pie_sub["slice_name"] = capitalize_abbreviations(f"Phân Bổ Phụ Theo {to_vietnamese_label(sub_cat_res)}")
    charts.append((pie_sub, pie_sub_uuid))

    # HÀNG 4: SEARCHABLE DIRECTORY TABLE (Width 12 - Ở cuối cùng)
    all_cols = [c for c in valid_cols if not c.startswith("_")]
    table_chart, table_uuid = build_chart_table(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        all_columns=all_cols,
        entity_label=entity_label,
        metric_columns=decision.metric_columns,
    )
    table_chart["slice_name"] = f"Danh Bạ Hồ Sơ Chi Tiết {entity_label}"
    charts.append((table_chart, table_uuid))

    # Grid Position
    position: Dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "children": ["GRID_ID"], "id": "ROOT_ID"},
        "GRID_ID": {
            "type": "GRID",
            "children": [
                "ROW-CATALOG-KPI-1",
                "ROW-CATALOG-CHARTS-2",
                "ROW-CATALOG-CHARTS-3",
                "ROW-OPERATIONAL-4",
            ],
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
        },
    }
    kpi_children = ["CHART-CATALOG-KPI-1", "CHART-CATALOG-KPI-2", "CHART-CATALOG-KPI-3", "CHART-CATALOG-KPI-4"]
    for idx, c_tuple in enumerate([(kpi_cnt, kpi_cnt_uuid), (kpi_p_avg, kpi_p_avg_uuid), (kpi_p_max, kpi_p_max_uuid), (kpi_r_avg, kpi_r_avg_uuid)]):
        c_data, c_uuid = c_tuple
        k_id = f"CHART-CATALOG-KPI-{idx + 1}"
        position[k_id] = {
            "children": [], "id": k_id,
            "meta": {"chartId": 201 + idx, "height": 22, "sliceName": c_data["slice_name"], "uuid": c_uuid, "width": 3},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-CATALOG-KPI-1"], "type": "CHART"
        }
    position["ROW-CATALOG-KPI-1"] = {
        "children": kpi_children, "id": "ROW-CATALOG-KPI-1", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-CATALOG-VISUAL-1"] = {
        "children": [], "id": "CHART-CATALOG-VISUAL-1",
        "meta": {"chartId": 211, "height": 48, "sliceName": pie_cat["slice_name"], "uuid": pie_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-CATALOG-CHARTS-2"], "type": "CHART"
    }
    position["CHART-CATALOG-VISUAL-2"] = {
        "children": [], "id": "CHART-CATALOG-VISUAL-2",
        "meta": {"chartId": 212, "height": 48, "sliceName": bar_rank["slice_name"], "uuid": bar_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-CATALOG-CHARTS-2"], "type": "CHART"
    }
    position["ROW-CATALOG-CHARTS-2"] = {
        "children": ["CHART-CATALOG-VISUAL-1", "CHART-CATALOG-VISUAL-2"], "id": "ROW-CATALOG-CHARTS-2", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-CATALOG-VISUAL-3"] = {
        "children": [], "id": "CHART-CATALOG-VISUAL-3",
        "meta": {"chartId": 221, "height": 48, "sliceName": bar_cat_stat["slice_name"], "uuid": bar_cstat_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-CATALOG-CHARTS-3"], "type": "CHART"
    }
    position["CHART-CATALOG-VISUAL-4"] = {
        "children": [], "id": "CHART-CATALOG-VISUAL-4",
        "meta": {"chartId": 222, "height": 48, "sliceName": pie_sub["slice_name"], "uuid": pie_sub_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-CATALOG-CHARTS-3"], "type": "CHART"
    }
    position["ROW-CATALOG-CHARTS-3"] = {
        "children": ["CHART-CATALOG-VISUAL-3", "CHART-CATALOG-VISUAL-4"], "id": "ROW-CATALOG-CHARTS-3", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-OPERATIONAL-TABLE"] = {
        "children": [], "id": "CHART-OPERATIONAL-TABLE",
        "meta": {"chartId": 299, "height": 60, "sliceName": table_chart["slice_name"], "uuid": table_uuid, "width": 12},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-OPERATIONAL-4"], "type": "CHART"
    }
    position["ROW-OPERATIONAL-4"] = {
        "children": ["CHART-OPERATIONAL-TABLE"], "id": "ROW-OPERATIONAL-4", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }

    title = f"[Auto] {entity_label} Catalog & Attribute Analytics"
    desc = f"Dashboard danh mục và hồ sơ thực thể cho {entity_label}."
    return charts, position, title, desc, {}


def _build_layout_operational_performance(
    decision: RoutingDecision,
    dataset_uuid: str,
    valid_cols: Set[str],
    table_name: str,
    entity_label: str,
    _resolve,
) -> Tuple[List[Tuple[Dict[str, Any], str]], Dict[str, Any], str, str, Dict[str, str]]:
    """Overview for multi-entity operational contexts without inventing risk fields."""
    charts: List[Tuple[Dict[str, Any], str]] = []
    key = _resolve(decision.business_keys[0] if decision.business_keys else "id")
    dimensions = [
        _resolve(to_snake_case(column))
        for column in decision.dimension_columns
        if not is_technical_column(column)
    ]
    dimensions = list(dict.fromkeys(column for column in dimensions if column in valid_cols))

    def metric_info(name: str) -> Tuple[str, str, str, str]:
        field = _resolve(to_snake_case(name), fallback=key)
        average = is_average_metric(name)
        label = to_vietnamese_label(name)
        if average and label.startswith("Bình Quân "):
            label = label[len("Bình Quân "):]
        return (
            field,
            label,
            "AVG" if average else "SUM",
            ",.1f" if average else ",.0f",
        )

    metric_infos = [metric_info(name) for name in decision.metric_columns[:3]]

    count_chart, count_uuid = build_chart_kpi(
        table_name, dataset_uuid, "op_total_entities", "Tổng Số Đối Tượng",
        key, "COUNT", ",.0f",
    )
    charts.append((count_chart, count_uuid))

    for index in range(3):
        if index < len(metric_infos):
            field, label, aggregate, number_format = metric_infos[index]
            chart, chart_uuid = build_chart_kpi(
                table_name, dataset_uuid, f"op_metric_{index + 1}", label,
                field, aggregate, number_format,
            )
        else:
            chart, chart_uuid = build_chart_kpi(
                table_name, dataset_uuid, f"op_count_{index + 1}", "Tổng Số Bản Ghi",
                key, "COUNT", ",.0f",
            )
        charts.append((chart, chart_uuid))

    visual_charts: List[Tuple[Dict[str, Any], str]] = []
    if dimensions:
        pie, pie_uuid = build_chart_pie(
            table_name, dataset_uuid, dimensions[0], to_vietnamese_label(dimensions[0]),
            key, "Số lượng", "COUNT", ",.0f",
        )
        visual_charts.append((pie, pie_uuid))
    for index, info in enumerate(metric_infos[:2]):
        dimension = dimensions[min(index, len(dimensions) - 1)] if dimensions else key
        field, label, aggregate, number_format = info
        bar, bar_uuid = build_chart_bar(
            table_name, dataset_uuid, dimension, to_vietnamese_label(dimension),
            field, label, aggregate, number_format, True,
        )
        visual_charts.append((bar, bar_uuid))
    charts.extend(visual_charts)

    detail_columns = [
        column for column in sorted(valid_cols)
        if not column.startswith("_") and not is_technical_column(column)
    ]
    table_chart, table_uuid = build_chart_table(
        table_name, dataset_uuid, detail_columns, entity_label, decision.metric_columns,
    )
    table_chart["slice_name"] = f"Chi Tiết Vận Hành {entity_label}"
    charts.append((table_chart, table_uuid))

    position: Dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "children": ["GRID_ID"], "id": "ROOT_ID"},
        "GRID_ID": {
            "type": "GRID", "children": ["ROW-OP-KPI", "ROW-OP-VISUAL", "ROW-OP-TABLE"],
            "id": "GRID_ID", "parents": ["ROOT_ID"],
        },
    }
    kpi_children = []
    for index, (chart, chart_uuid) in enumerate(charts[:4], 1):
        item_id = f"CHART-OP-KPI-{index}"
        kpi_children.append(item_id)
        position[item_id] = {
            "children": [], "id": item_id,
            "meta": {"chartId": 401 + index, "height": 22, "sliceName": chart["slice_name"], "uuid": chart_uuid, "width": 3},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-OP-KPI"], "type": "CHART",
        }
    position["ROW-OP-KPI"] = {
        "children": kpi_children, "id": "ROW-OP-KPI", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW",
    }
    visual_children = []
    for index, (chart, chart_uuid) in enumerate(visual_charts, 1):
        item_id = f"CHART-OP-VISUAL-{index}"
        visual_children.append(item_id)
        position[item_id] = {
            "children": [], "id": item_id,
            "meta": {"chartId": 410 + index, "height": 48, "sliceName": chart["slice_name"], "uuid": chart_uuid, "width": 4},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-OP-VISUAL"], "type": "CHART",
        }
    position["ROW-OP-VISUAL"] = {
        "children": visual_children, "id": "ROW-OP-VISUAL", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW",
    }
    position["CHART-OP-TABLE"] = {
        "children": [], "id": "CHART-OP-TABLE",
        "meta": {"chartId": 499, "height": 60, "sliceName": table_chart["slice_name"], "uuid": table_uuid, "width": 12},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-OP-TABLE"], "type": "CHART",
    }
    position["ROW-OP-TABLE"] = {
        "children": ["CHART-OP-TABLE"], "id": "ROW-OP-TABLE", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW",
    }
    title = f"[Auto] {entity_label} – Tổng Quan Vận Hành"
    description = (
        f"Dashboard vận hành đa thực thể cho {entity_label}. "
        f"Router confidence={decision.dashboard_routing_confidence}; "
        f"signals={', '.join(decision.dashboard_routing_signals)}."
    )
    return charts, position, title, description, {}


def _build_layout_categorical_distribution(
    decision: RoutingDecision,
    dataset_uuid: str,
    valid_cols: Set[str],
    table_name: str,
    entity_label: str,
    primary_eval_metric: Optional[str],
    rank_metric: Optional[str],
    _resolve,
) -> Tuple[List[Tuple[Dict[str, Any], str]], Dict[str, Any], str, str, Dict[str, str]]:
    """Layout 4: Phân loại & Khảo sát (4 Tầng: Frequency KPIs -> Primary Share -> Sub Share -> Table)."""
    charts: List[Tuple[Dict[str, Any], str]] = []

    # Sắp xếp thứ tự ưu tiên các Dimension:
    # 1. Dimension biểu thị kết quả/đánh giá: satisfaction, review, rating, status, sentiment, feedback
    # 2. Dimension phân khúc đối tượng: customer_type, class, segment, tier, category, type
    # 3. Dimension nhân khẩu học / tác vụ: travel_type, gender, role, channel
    all_dims = [d for d in decision.dimension_columns if not d.startswith("_")]
    primary_survey_dims = [d for d in all_dims if any(k in d.lower() for k in ["satisfaction", "feedback", "rating", "review", "sentiment", "hai_long", "danh_gia"])]
    segment_dims = [d for d in all_dims if any(k in d.lower() for k in ["customer", "class", "tier", "segment", "loai", "hang", "nhom"])]
    other_dims = [d for d in all_dims if d not in primary_survey_dims and d not in segment_dims]

    ordered_dims = primary_survey_dims + segment_dims + other_dims
    if not ordered_dims:
        ordered_dims = all_dims or ["nhom_don_vi"]

    dim1 = _resolve(to_snake_case(ordered_dims[0]))
    dim2 = _resolve(to_snake_case(ordered_dims[1])) if len(ordered_dims) > 1 else dim1
    dim3 = _resolve(to_snake_case(ordered_dims[2])) if len(ordered_dims) > 2 else dim2
    dim4 = _resolve(to_snake_case(ordered_dims[3])) if len(ordered_dims) > 3 else dim1

    lbl1 = to_vietnamese_label(dim1)
    lbl2 = to_vietnamese_label(dim2)
    lbl3 = to_vietnamese_label(dim3)
    lbl4 = to_vietnamese_label(dim4)

    # HÀNG 1: DISTRIBUTION KPIS (4 Cards, Width 3 each)
    kpi_cnt, kpi_cnt_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="dist_total_records",
        metric_label="Tổng Số Lượt Bản Ghi",
        column_name=_resolve("total_records"),
        aggregate="SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_cnt, kpi_cnt_uuid))

    kpi_cat1, kpi_cat1_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="dist_dim1_count",
        metric_label=f"Quy Mô {lbl1}",
        column_name=_resolve("total_records"),
        aggregate="SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_cat1, kpi_cat1_uuid))

    kpi_cat2, kpi_cat2_uuid = build_chart_kpi(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        metric_name="dist_dim2_count",
        metric_label=f"Phân Lớp {lbl2}",
        column_name=_resolve("total_records"),
        aggregate="SUM",
        y_axis_format=",.0f",
    )
    charts.append((kpi_cat2, kpi_cat2_uuid))

    # Card 4: Nếu có metric đánh giá/đo lường chính thì hiển thị TB chỉ số, ngược lại hiển thị tổng số
    if primary_eval_metric:
        p_snake = to_snake_case(primary_eval_metric)
        p_label = to_vietnamese_label(primary_eval_metric)
        is_p_avg = is_average_metric(primary_eval_metric)
        p_avg_col = _resolve(f"avg_{p_snake}", fallback=_resolve(p_snake, "total_records"))
        kpi_rate, kpi_rate_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="dist_metric_eval",
            metric_label=f"TB {p_label}" if is_p_avg or p_avg_col.startswith("avg_") else f"Tổng {p_label}",
            column_name=p_avg_col,
            aggregate="AVG" if p_avg_col.startswith("avg_") else "SUM",
            y_axis_format=",.1f" if is_p_avg or p_avg_col.startswith("avg_") else ",.0f",
        )
    else:
        kpi_rate, kpi_rate_uuid = build_chart_kpi(
            table_name=table_name,
            dataset_uuid=dataset_uuid,
            metric_name="dist_valid_rate",
            metric_label="Chỉ Số Quy Mô Tổng Thể",
            column_name=_resolve("total_records"),
            aggregate="SUM",
            y_axis_format=",.0f",
        )
    charts.append((kpi_rate, kpi_rate_uuid))

    # HÀNG 2: PRIMARY DISTRIBUTION VISUALS (Width 6 & 6)
    pie_1, pie_1_uuid = build_chart_pie(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=dim1,
        dimension_label=lbl1,
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        number_format=",.0f",
    )
    charts.append((pie_1, pie_1_uuid))

    bar_2, bar_2_uuid = build_chart_bar(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=dim2,
        dimension_label=lbl2,
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        y_axis_format=",.0f",
        is_top_10=True,
    )
    charts.append((bar_2, bar_2_uuid))

    # HÀNG 3: SUB DISTRIBUTION VISUALS (Width 6 & 6)
    pie_3, pie_3_uuid = build_chart_pie(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=dim3,
        dimension_label=lbl3,
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        number_format=",.0f",
    )
    charts.append((pie_3, pie_3_uuid))

    bar_sub, bar_sub_uuid = build_chart_bar(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        dimension_col=dim4,
        dimension_label=lbl4,
        metric_col=_resolve("total_records"),
        metric_label="Số lượng",
        aggregate="SUM",
        y_axis_format=",.0f",
        is_top_10=False,
    )
    charts.append((bar_sub, bar_sub_uuid))

    # HÀNG 4: DETAIL TABLE (Width 12 - Ở cuối cùng)
    all_cols = [c for c in valid_cols if not c.startswith("_")]
    table_chart, table_uuid = build_chart_table(
        table_name=table_name,
        dataset_uuid=dataset_uuid,
        all_columns=all_cols,
        entity_label=entity_label,
        metric_columns=decision.metric_columns,
    )
    table_chart["slice_name"] = f"Chi Tiết Dữ Liệu Khảo Sát & Phân Loại {entity_label}"
    charts.append((table_chart, table_uuid))

    # Grid Position
    position: Dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "children": ["GRID_ID"], "id": "ROOT_ID"},
        "GRID_ID": {
            "type": "GRID",
            "children": [
                "ROW-SURVEY-KPI-1",
                "ROW-SURVEY-CHARTS-2",
                "ROW-SURVEY-CHARTS-3",
                "ROW-OPERATIONAL-4",
            ],
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
        },
    }
    kpi_children = ["CHART-SURVEY-KPI-1", "CHART-SURVEY-KPI-2", "CHART-SURVEY-KPI-3", "CHART-SURVEY-KPI-4"]
    for idx, c_tuple in enumerate([(kpi_cnt, kpi_cnt_uuid), (kpi_cat1, kpi_cat1_uuid), (kpi_cat2, kpi_cat2_uuid), (kpi_rate, kpi_rate_uuid)]):
        c_data, c_uuid = c_tuple
        k_id = f"CHART-SURVEY-KPI-{idx + 1}"
        position[k_id] = {
            "children": [], "id": k_id,
            "meta": {"chartId": 301 + idx, "height": 22, "sliceName": c_data["slice_name"], "uuid": c_uuid, "width": 3},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-SURVEY-KPI-1"], "type": "CHART"
        }
    position["ROW-SURVEY-KPI-1"] = {
        "children": kpi_children, "id": "ROW-SURVEY-KPI-1", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-SURVEY-VISUAL-1"] = {
        "children": [], "id": "CHART-SURVEY-VISUAL-1",
        "meta": {"chartId": 311, "height": 48, "sliceName": pie_1["slice_name"], "uuid": pie_1_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-SURVEY-CHARTS-2"], "type": "CHART"
    }
    position["CHART-SURVEY-VISUAL-2"] = {
        "children": [], "id": "CHART-SURVEY-VISUAL-2",
        "meta": {"chartId": 312, "height": 48, "sliceName": bar_2["slice_name"], "uuid": bar_2_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-SURVEY-CHARTS-2"], "type": "CHART"
    }
    position["ROW-SURVEY-CHARTS-2"] = {
        "children": ["CHART-SURVEY-VISUAL-1", "CHART-SURVEY-VISUAL-2"], "id": "ROW-SURVEY-CHARTS-2", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-SURVEY-VISUAL-3"] = {
        "children": [], "id": "CHART-SURVEY-VISUAL-3",
        "meta": {"chartId": 321, "height": 48, "sliceName": pie_3["slice_name"], "uuid": pie_3_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-SURVEY-CHARTS-3"], "type": "CHART"
    }
    position["CHART-SURVEY-VISUAL-4"] = {
        "children": [], "id": "CHART-SURVEY-VISUAL-4",
        "meta": {"chartId": 322, "height": 48, "sliceName": bar_sub["slice_name"], "uuid": bar_sub_uuid, "width": 6},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-SURVEY-CHARTS-3"], "type": "CHART"
    }
    position["ROW-SURVEY-CHARTS-3"] = {
        "children": ["CHART-SURVEY-VISUAL-3", "CHART-SURVEY-VISUAL-4"], "id": "ROW-SURVEY-CHARTS-3", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }
    position["CHART-OPERATIONAL-TABLE"] = {
        "children": [], "id": "CHART-OPERATIONAL-TABLE",
        "meta": {"chartId": 399, "height": 60, "sliceName": table_chart["slice_name"], "uuid": table_uuid, "width": 12},
        "parents": ["ROOT_ID", "GRID_ID", "ROW-OPERATIONAL-4"], "type": "CHART"
    }
    position["ROW-OPERATIONAL-4"] = {
        "children": ["CHART-OPERATIONAL-TABLE"], "id": "ROW-OPERATIONAL-4", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"
    }

    title = f"[Auto] {entity_label} Distribution & Survey Analytics"
    desc = f"Dashboard phân tích cơ cấu và khảo sát tần suất cho {entity_label}."
    return charts, position, title, desc, {}


# =====================================================================
# BUNDLE GENERATOR
# =====================================================================

def build_dashboard_bundle(decision: RoutingDecision) -> Dict[str, Any]:
    """Sinh toàn bộ các entities (Database, Dataset, Charts, Dashboard) sẵn sàng export.
    Tự động nhận diện Archetype nghiệp vụ để áp dụng Layout Dashboard tối ưu nhất:
    1. PERFORMANCE_RISK: Bố cục 5 tầng cho KPI, Ngoại lệ, Rủi ro, Quyết định vận hành.
    2. TIME_SERIES: Bố cục 4 tầng tập trung Biểu đồ xu hướng chuỗi thời gian & Biến thiên.
    3. ENTITY_CATALOG: Bố cục 4 tầng cho Danh mục hồ sơ thực thể (Nhân vật, Thiết bị, Sinh viên).
    4. CATEGORICAL_DISTRIBUTION: Bố cục 4 tầng cho Khảo sát, Đánh giá và Tần suất phân loại.
    """
    table_name = decision.target_gold_table.split(".")[-1]
    entity_label = format_entity_title(decision.dataset_entity)
    dashboard_uuid = get_deterministic_uuid(f"dashboard.{table_name}")

    # 1. Dataset
    dataset_dict, dataset_uuid = build_dataset_yaml(decision)
    valid_cols = {c["column_name"].lower() for c in dataset_dict["columns"]}

    def _resolve(pref: str, fallback: str = "total_records") -> str:
        if pref.lower() in valid_cols:
            return pref
        normalized = to_snake_case(pref)
        physical_matches = sorted(c for c in valid_cols if to_snake_case(c) == normalized)
        if physical_matches:
            return physical_matches[0]
        if fallback.lower() in valid_cols:
            return fallback
        fallback_normalized = to_snake_case(fallback)
        fallback_matches = sorted(c for c in valid_cols if to_snake_case(c) == fallback_normalized)
        if fallback_matches:
            return fallback_matches[0]
        return list(valid_cols)[0] if valid_cols else "total_records"

    # Xác định Metric chính (Primary Outcome Metric)
    priority_eval_keywords = [
        "temp", "temperature", "nhiet_do", "pulled_count", "amount_paid", "tuition_amount", "revenue",
        "sales", "diem_tb", "gpa", "lvl_90_atk", "score", "so_tcdk", "diem_tbrl", "humidity", "do_am"
    ]
    primary_eval_metric = None
    for kw in priority_eval_keywords:
        for m in decision.metric_columns:
            if kw in m.lower():
                primary_eval_metric = m
                break
        if primary_eval_metric:
            break

    # Xử lý các từ khóa ngắn đặc thù IoT (dùng word boundary để tránh match nhầm như 'co' trong 'convenient' hay 'comfort')
    if not primary_eval_metric and (decision.dataset_domain or "").lower() == "iot":
        for kw in ["co", "smoke", "lpg", "gas"]:
            for m in decision.metric_columns:
                m_words = m.lower().replace('/', ' ').replace('_', ' ').split()
                if kw in m_words:
                    primary_eval_metric = m
                    break
            if primary_eval_metric:
                break

    if not primary_eval_metric and decision.metric_columns:
        primary_eval_metric = decision.metric_columns[0]

    # Xác định Metric hiệu suất/xếp hạng (Efficiency / Rank Metric)
    rank_metric = None
    for kw in ["humidity", "do_am", "atk", "damage", "dmg", "dps", "diem_tb", "gpa", "tbrl", "score", "amount", "pulled_count", "hp"]:
        for m in decision.metric_columns:
            if kw in m.lower() and m != primary_eval_metric:
                rank_metric = m
                break
        if rank_metric:
            break
    if not rank_metric and (decision.dataset_domain or "").lower() == "iot":
        for kw in ["co", "smoke", "lpg"]:
            for m in decision.metric_columns:
                m_words = m.lower().replace('/', ' ').replace('_', ' ').split()
                if kw in m_words and m != primary_eval_metric:
                    rank_metric = m
                    break
            if rank_metric:
                break
    if not rank_metric:
        rank_metric = primary_eval_metric

    # 2. Tự động nhận diện Archetype
    archetype = detect_dashboard_archetype(decision, valid_cols)
    print(f"🎯 [Superset Provisioner] Nhận diện Dashboard Archetype: {archetype.value.upper()} cho dataset: {decision.dataset_entity}")

    # 3. Tuyển chọn Layout Generator tương ứng
    if archetype == DashboardArchetype.PERFORMANCE_RISK:
        charts, position, title, desc, label_colors = _build_layout_performance_risk(
            decision=decision,
            dataset_uuid=dataset_uuid,
            valid_cols=valid_cols,
            table_name=table_name,
            entity_label=entity_label,
            primary_eval_metric=primary_eval_metric,
            rank_metric=rank_metric,
            _resolve=_resolve,
        )
    elif archetype == DashboardArchetype.OPERATIONAL_PERFORMANCE:
        charts, position, title, desc, label_colors = _build_layout_operational_performance(
            decision=decision,
            dataset_uuid=dataset_uuid,
            valid_cols=valid_cols,
            table_name=table_name,
            entity_label=entity_label,
            _resolve=_resolve,
        )
    elif archetype == DashboardArchetype.TIME_SERIES:
        charts, position, title, desc, label_colors = _build_layout_time_series(
            decision=decision,
            dataset_uuid=dataset_uuid,
            valid_cols=valid_cols,
            table_name=table_name,
            entity_label=entity_label,
            primary_eval_metric=primary_eval_metric,
            rank_metric=rank_metric,
            _resolve=_resolve,
        )
    elif archetype == DashboardArchetype.ENTITY_CATALOG:
        charts, position, title, desc, label_colors = _build_layout_entity_catalog(
            decision=decision,
            dataset_uuid=dataset_uuid,
            valid_cols=valid_cols,
            table_name=table_name,
            entity_label=entity_label,
            primary_eval_metric=primary_eval_metric,
            rank_metric=rank_metric,
            _resolve=_resolve,
        )
    else:
        charts, position, title, desc, label_colors = _build_layout_categorical_distribution(
            decision=decision,
            dataset_uuid=dataset_uuid,
            valid_cols=valid_cols,
            table_name=table_name,
            entity_label=entity_label,
            primary_eval_metric=primary_eval_metric,
            rank_metric=rank_metric,
            _resolve=_resolve,
        )

    slug = f"auto-{table_name.replace('_', '-')}"
    dashboard_dict = {
        "dashboard_title": title,
        "description": desc,
        "css": "",
        "slug": slug,
        "certified_by": "CTU AI Data Lakehouse",
        "certification_details": f"Template Archetype: {archetype.value}",
        "published": True,
        "uuid": dashboard_uuid,
        "position": position,
        "metadata": {
            "chart_configuration": {},
            "color_scheme": "supersetColors",
            "label_colors": label_colors,
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "default_filters": "{}",
        },
        "version": "1.0.0",
    }

    # 4. Database Definition
    database_dict = {
        "database_name": "CTU IOC Trino",
        "sqlalchemy_uri": "trino://superset@trino:8080/lakehouse/gold",
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_file_upload": False,
        "extra": {
            "metadata_params": {},
            "engine_params": {},
            "metadata_cache_timeout": {},
            "schemas_allowed_for_file_upload": [],
        },
        "uuid": DATABASE_UUID,
        "version": "1.0.0",
    }

    return {
        "database": database_dict,
        "dataset": dataset_dict,
        "charts": charts,
        "dashboard": dashboard_dict,
        "dashboard_uuid": dashboard_uuid,
        "table_name": table_name,
        "dashboard_title": title,
        "archetype": archetype.value,
    }


def package_to_zip(bundle: Dict[str, Any], output_zip_path: Path) -> Path:
    """Đóng gói toàn bộ cấu hình vào file ZIP theo chuẩn Superset Import/Export."""
    output_zip_path.parent.mkdir(parents=True, exist_ok=True)
    table_name = bundle["table_name"]

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    folder_prefix = f"dashboard_export_{timestamp}"

    metadata_yaml = yaml.dump({
        "version": "1.0.0",
        "type": "Dashboard",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }, sort_keys=False, allow_unicode=True)

    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        # 1. metadata.yaml
        z.writestr(f"{folder_prefix}/metadata.yaml", metadata_yaml)

        # 2. databases/CTU_IOC_Trino.yaml
        z.writestr(
            f"{folder_prefix}/databases/CTU_IOC_Trino.yaml",
            yaml.dump(bundle["database"], sort_keys=False, allow_unicode=True),
        )

        # 3. datasets/CTU_IOC_Trino/<table_name>.yaml
        z.writestr(
            f"{folder_prefix}/datasets/CTU_IOC_Trino/{table_name}.yaml",
            yaml.dump(bundle["dataset"], sort_keys=False, allow_unicode=True),
        )

        # 4. charts
        for chart_data, c_uuid in bundle["charts"]:
            safe_name = chart_data["slice_name"].replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
            z.writestr(
                f"{folder_prefix}/charts/{safe_name}_{c_uuid[:8]}.yaml",
                yaml.dump(chart_data, sort_keys=False, allow_unicode=True),
            )

        # 5. dashboards/<dashboard_slug>.yaml
        slug = bundle["dashboard"]["slug"]
        z.writestr(
            f"{folder_prefix}/dashboards/{slug}.yaml",
            yaml.dump(bundle["dashboard"], sort_keys=False, allow_unicode=True),
        )

    print(f"📦 [Superset Provisioner] Đã đóng gói thành công file ZIP: {output_zip_path}")
    return output_zip_path


def import_into_superset_api(
    zip_path: Path,
    slug: str,
    table_name: Optional[str] = None,
    base_url: Optional[str] = None,
    username: str = "admin",
    password: str = "admin",
) -> bool:
    """Import trực tiếp qua Superset REST API (/api/v1/dashboard/import/).
    Tự động dọn dẹp dashboard cũ cùng slug để loại bỏ triệt để lát cắt thừa trước khi nạp mới.
    """
    candidate_urls = []
    if base_url:
        candidate_urls.append(base_url.rstrip("/"))
    if os.environ.get("SUPERSET_API_URL"):
        candidate_urls.append(os.environ["SUPERSET_API_URL"].rstrip("/"))
    candidate_urls.extend(["http://superset:8088", "http://localhost:8088", "http://127.0.0.1:8088"])

    target_url = None
    for url in candidate_urls:
        try:
            h = requests.get(f"{url}/health", timeout=2)
            if h.status_code == 200:
                target_url = url
                break
        except Exception:
            continue

    if not target_url:
        print("⚠️ [Superset Provisioner] Không tìm thấy URL Superset phản hồi endpoint /health.")
        return False

    print(f"🌐 [Superset Provisioner] Kết nối tới Superset REST API tại: {target_url}...")
    try:
        login_resp = requests.post(
            f"{target_url}/api/v1/security/login",
            json={"username": username, "password": password, "provider": "db", "refresh": True},
            timeout=10,
        )
        if login_resp.status_code != 200:
            print(f"❌ [Superset Provisioner] Đăng nhập API thất bại ({login_resp.status_code}): {login_resp.text}")
            return False

        token = login_resp.json().get("access_token")
        headers = {"Authorization": f"Bearer {token}"}

        # Dọn dẹp dashboard & dataset cũ cùng table_name/slug để đảm bảo layout và column verbose_name mới tinh
        try:
            # 1. Tìm dashboard cũ và xóa các chart thuộc dashboard đó trước
            get_resp = requests.get(
                f"{target_url}/api/v1/dashboard/?q=(filters:!((col:slug,opr:eq,value:'{slug}')))",
                headers=headers,
                timeout=10,
            )
            if get_resp.status_code == 200:
                res = get_resp.json().get("result", [])
                for d in res:
                    d_id = d.get("id")
                    if d_id:
                        dash_detail = requests.get(f"{target_url}/api/v1/dashboard/{d_id}", headers=headers, timeout=10)
                        if dash_detail.status_code == 200:
                            pos_raw = dash_detail.json().get("result", {}).get("position_json", "{}")
                            try:
                                pos_dict = json.loads(pos_raw)
                                for nk, nv in pos_dict.items():
                                    if nk.startswith("CHART-"):
                                        cid = nv.get("meta", {}).get("chartId")
                                        if cid:
                                            requests.delete(f"{target_url}/api/v1/chart/{cid}", headers=headers, timeout=10)
                            except Exception:
                                pass
                        print(f"🧹 [Superset Provisioner] Xóa dashboard cũ ID {d_id} ({slug}) để nạp cấu hình mới...")
                        requests.delete(f"{target_url}/api/v1/dashboard/{d_id}", headers=headers, timeout=10)

            # 2. Xóa dataset cũ để Superset cập nhật lại toàn bộ verbose_name tiếng Việt có dấu
            if table_name:
                get_ds = requests.get(
                    f"{target_url}/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:'{table_name}')))",
                    headers=headers,
                    timeout=10,
                )
                if get_ds.status_code == 200:
                    res_ds = get_ds.json().get("result", [])
                    for ds in res_ds:
                        ds_id = ds.get("id")
                        if ds_id:
                            # Xóa các chart gắn với dataset này trước để tránh chart mồ côi
                            try:
                                get_ds_charts = requests.get(
                                    f"{target_url}/api/v1/chart/?q=(filters:!((col:datasource_id,opr:eq,value:{ds_id})))",
                                    headers=headers,
                                    timeout=10,
                                )
                                if get_ds_charts.status_code == 200:
                                    for ch in get_ds_charts.json().get("result", []):
                                        if ch.get("id"):
                                            requests.delete(f"{target_url}/api/v1/chart/{ch['id']}", headers=headers, timeout=10)
                            except Exception:
                                pass
                            print(f"🧹 [Superset Provisioner] Xóa dataset cũ ID {ds_id} ({table_name}) để nạp mới verbose_name...")
                            requests.delete(f"{target_url}/api/v1/dataset/{ds_id}", headers=headers, timeout=10)

                # 3. Xóa mọi chart cũ còn lại liên quan đến table_name
                get_charts = requests.get(
                    f"{target_url}/api/v1/chart/?q=(filters:!((col:description,opr:ct,value:'{table_name}')),page_size:100)",
                    headers=headers,
                    timeout=10,
                )
                if get_charts.status_code == 200:
                    res_ch = get_charts.json().get("result", [])
                    for ch in res_ch:
                        ch_id = ch.get("id")
                        if ch_id:
                            requests.delete(f"{target_url}/api/v1/chart/{ch_id}", headers=headers, timeout=10)
        except Exception as ce:
            print(f"ℹ️ [Superset Provisioner] Bỏ qua bước kiểm tra dọn dẹp: {ce}")

        with open(zip_path, "rb") as zf:
            files = {"formData": (zip_path.name, zf, "application/zip")}
            data = {"overwrite": "true", "passwords": "{}"}
            import_resp = requests.post(
                f"{target_url}/api/v1/dashboard/import/",
                headers=headers,
                files=files,
                data=data,
                timeout=30,
            )

        if import_resp.status_code == 200:
            print(f"✅ [Superset Provisioner] Import dashboard qua REST API thành công ({target_url})!")
            return True
        else:
            print(f"❌ [Superset Provisioner] Import API trả về lỗi ({import_resp.status_code}): {import_resp.text}")
            return False

    except Exception as e:
        print(f"⚠️ [Superset Provisioner] Ngoại lệ khi gọi Superset REST API: {e}")
        return False


def import_into_superset_container_cli(
    zip_path: Path,
    container_name: str = "demo-superset",
    username: str = "admin",
) -> bool:
    """Fallback: Copy file ZIP vào Docker container và gọi CLI import-dashboards."""
    try:
        res = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if container_name not in res.stdout:
            print(f"⚠️ [Superset Provisioner] Container '{container_name}' không tìm thấy qua docker ps.")
            return False

        container_tmp_zip = f"/tmp/{zip_path.name}"
        subprocess.run(["docker", "cp", str(zip_path), f"{container_name}:{container_tmp_zip}"], check=True)

        import_cmd = [
            "docker", "exec", "-u", "0", container_name,
            "superset", "import-dashboards", "-p", container_tmp_zip, "-u", username,
        ]
        import_res = subprocess.run(import_cmd, capture_output=True, text=True, check=False)
        if import_res.returncode == 0:
            print("✅ [Superset Provisioner] Import qua Docker CLI thành công!")
            return True
        return False
    except Exception as e:
        print(f"⚠️ [Superset Provisioner] Ngoại lệ khi chạy Docker CLI fallback: {e}")
        return False


def provision_dynamic_dashboard(
    decision: RoutingDecision,
    output_dir: Optional[Path] = None,
    auto_import: bool = True,
    superset_url: Optional[str] = None,
    container_name: str = "demo-superset",
) -> Dict[str, Any]:
    """Hàm API tổng hợp thực hiện toàn bộ quy trình: Slot-filling -> ZIP -> Import."""
    if output_dir is None:
        output_dir = _CURRENT_DIR / "superset_exports" / "generated"

    bundle = build_dashboard_bundle(decision)
    table_name = bundle["table_name"]
    slug = bundle["dashboard"]["slug"]
    zip_path = output_dir / f"{table_name}_dashboard_bundle.zip"

    package_to_zip(bundle, zip_path)

    import_success = False
    if auto_import:
        username = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
        password = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

        # 1. Thử qua REST API trước (kèm tự động dọn dẹp dashboard cũ để sạch layout)
        import_success = import_into_superset_api(
            zip_path=zip_path,
            slug=slug,
            table_name=table_name,
            base_url=superset_url,
            username=username,
            password=password,
        )

        # 2. Nếu REST API chưa được thì fallback sang Docker CLI
        if not import_success:
            print("ℹ️ [Superset Provisioner] Thử phương thức dự phòng Docker CLI...")
            import_success = import_into_superset_container_cli(
                zip_path=zip_path,
                container_name=container_name,
                username=username,
            )

    return {
        "status": "SUCCESS" if (import_success or not auto_import) else "ZIP_READY",
        "table_name": table_name,
        "dashboard_title": bundle["dashboard_title"],
        "dashboard_slug": slug,
        "dashboard_uuid": bundle["dashboard_uuid"],
        "zip_path": str(zip_path),
        "imported": import_success,
    }


def main():
    parser = argparse.ArgumentParser(description="Superset Dynamic Dashboard Provisioner")
    parser.add_argument("--decision-file", "-d", required=True, help="Đường dẫn file RoutingDecision JSON")
    parser.add_argument("--output-dir", "-o", help="Thư mục xuất file bundle ZIP")
    parser.add_argument("--no-import", action="store_true", help="Chỉ đóng gói ZIP, không gọi tự động import")
    parser.add_argument("--url", help="URL Superset API (ví dụ: http://localhost:8088)")
    parser.add_argument("--container", default="demo-superset", help="Tên Docker container của Superset")
    args = parser.parse_args()

    with open(args.decision_file, "r", encoding="utf-8") as f:
        decision = RoutingDecision(**json.load(f))

    out_dir = Path(args.output_dir) if args.output_dir else None
    result = provision_dynamic_dashboard(
        decision=decision,
        output_dir=out_dir,
        auto_import=not args.no_import,
        superset_url=args.url,
        container_name=args.container,
    )
    print("\n" + json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
