# -*- coding: utf-8 -*-
"""AI-Augmented Semantic Profiler & Router for Universal Data Lakehouse.

This module acts as the CONTROL PLANE router:
1. Samples incoming data (JSON, CSV, Parquet, Dicts)
2. Recognizes legacy/registered pipelines via fast deterministic rules
3. Leverages Gemini AI to infer schema, business keys, metrics, dimensions,
   and target Iceberg table names for any arbitrary new datasets
4. Provides deterministic rule-based fallback when Gemini API is unavailable.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from pydantic import BaseModel, Field

# Thêm thư mục hiện tại vào sys.path để nạp env_config và api_dataset_registry
_CURRENT_DIR = Path(__file__).resolve().parent
if str(_CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CURRENT_DIR))

# Đảm bảo console Windows in tiếng Việt UTF-8 không bị lỗi charmap
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    from env_config import GEMINI_API_KEY
except ImportError:
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Tùy chọn nạp registry cho registered API check
try:
    from api_dataset_registry import DATASETS
except ImportError:
    DATASETS = {}


# =====================================================================
# PYDANTIC OUTPUT MODELS
# =====================================================================

class SuggestedVisualization(BaseModel):
    chart_type: str = Field(description="Loại biểu đồ: bar, line, pie, table, big_number, scatter")
    x_axis_or_dimension: Optional[str] = Field(None, description="Cột trục hoành hoặc dimension gom nhóm")
    y_axis_or_metric: Optional[str] = Field(None, description="Cột số liệu tính toán (SUM/AVG/COUNT)")
    aggregation: Optional[str] = Field("SUM", description="Hàm gom nhóm: SUM, AVG, COUNT, MAX, MIN")
    title: str = Field(description="Tiêu đề biểu đồ gợi ý cho Superset")


class RoutingDecision(BaseModel):
    dataset_domain: str = Field(
        description="Lĩnh vực dữ liệu: education, kpi, finance, hr, admissions, operations, generic"
    )
    dataset_entity: str = Field(
        description="Tên thực thể chuẩn hóa tiếng Anh snake_case, ví dụ: student_scores, tuition_payments"
    )
    route_target: str = Field(
        description="Nhánh định tuyến: 'legacy_kpi' | 'registered_api' | 'generic_dynamic' | 'relational_context'"
    )
    registered_dataset_id: Optional[str] = Field(
        None, description="ID dataset đã đăng ký (nếu thuộc registered_api)"
    )
    target_silver_table: str = Field(
        description="Tên bảng Silver Iceberg đầy đủ: lakehouse.silver.<dataset_entity>"
    )
    target_quarantine_table: str = Field(
        description="Tên bảng cách ly bản ghi lỗi: lakehouse.silver.<dataset_entity>_quarantine"
    )
    target_gold_table: str = Field(
        description="Tên bảng Gold Data Mart: lakehouse.gold.<dataset_entity>_summary"
    )
    business_keys: List[str] = Field(
        default_factory=list,
        description="Danh sách các cột cấu thành Natural Primary Key (khóa chính nghiệp vụ)"
    )
    source_updated_at_field: Optional[str] = Field(
        None, description="Tên cột thể hiện thời điểm cập nhật mốc thời gian của dòng (nếu có)"
    )
    dimension_columns: List[str] = Field(
        default_factory=list,
        description="Danh sách các cột phân loại, danh mục, chuỗi, thời gian (Dimension)"
    )
    metric_columns: List[str] = Field(
        default_factory=list,
        description="Danh sách các cột số liệu đo lường định lượng (Metric)"
    )
    suggested_visualizations: List[SuggestedVisualization] = Field(
        default_factory=list,
        description="Gợi ý các biểu đồ trực quan hóa phù hợp cho Superset"
    )
    is_existing_table_match: bool = Field(
        False,
        description="True nếu dữ liệu mới này được nhận diện là phiên bản mở rộng/tiến hóa của một bảng Silver đã có"
    )
    column_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="Bản đồ đổi tên cột từ file nguồn sang cột bảng cũ (ví dụ: {'student_code': 'student_id'})"
    )
    new_columns: List[str] = Field(
        default_factory=list,
        description="Danh sách các cột mới toanh xuất hiện lần đầu cần thêm vào bảng cũ qua Iceberg Schema Evolution"
    )
    fallback_used: bool = Field(
        False, description="True nếu phải sử dụng Rule-based Inferrer do AI không khả dụng"
    )
    extracted_json_path: Optional[str] = Field(
        None, description="Đường dẫn file JSON đã bóc tách từ tài liệu phi cấu trúc (nếu có)"
    )
    reasoning: str = Field(
        description="Giải thích ngắn gọn lý do đưa ra quyết định định tuyến"
    )


# =====================================================================
# UTILITY FUNCTIONS: SANITIZATION & SAMPLING
# =====================================================================

def to_snake_case(text: str) -> str:
    """Chuyển chuỗi tiếng Việt hoặc hoa/thường thành snake_case không dấu."""
    # Bỏ dấu tiếng Việt
    nfkd = unicodedata.normalize("NFKD", text)
    cleaned = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = cleaned.replace("đ", "d").replace("Đ", "d")
    # Thay ký tự đặc biệt bằng dấu gạch dưới
    cleaned = re.sub(r"[^\w\s]", "_", cleaned)
    # Tách CamelCase
    cleaned = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", cleaned)
    # Thu gọn khoảng trắng và gạch dưới
    cleaned = re.sub(r"[\s_]+", "_", cleaned).strip("_").lower()
    return cleaned or "col"


def guess_value_type(val: Any) -> str:
    """Đoán kiểu dữ liệu thô của giá trị mẫu."""
    if val is None or val == "":
        return "null"
    if isinstance(val, bool):
        return "boolean"
    if isinstance(val, (int, float)):
        return "number"
    val_str = str(val).strip()
    # Kiểm tra số dạng string
    if re.match(r"^-?\d+$", val_str):
        return "integer"
    if re.match(r"^-?\d+\.\d+$", val_str):
        return "double"
    # Kiểm tra ngày tháng
    if re.match(r"^\d{4}-\d{2}-\d{2}", val_str):
        return "timestamp"
    return "string"


# =====================================================================
# PRE-ROUTING CHECKS: LEGACY KPI & REGISTERED APIS
# =====================================================================

LEGACY_KPI_COLUMNS = {
    "ma_chi_tieu", "muc_dang_ky", "muc_dat", "ket_qua_he_thong",
    "nhom_don_vi", "quy_danh_gia", "noi_dung_muc_tieu"
}


def check_legacy_kpi_match(columns: List[str], filename: str = "") -> Optional[RoutingDecision]:
    """Kiểm tra xem dữ liệu có khớp cấu trúc KPI CUSC truyền thống không."""
    col_set = {to_snake_case(c) for c in columns}
    # Khớp ít nhất 3 cột KPI đặc thù
    matching_kpi = col_set.intersection(LEGACY_KPI_COLUMNS)
    fname_lower = filename.lower()
    is_kpi_file = any(kw in fname_lower for kw in ["kpi", "cusc", "danh_gia_muc_tieu"])
    is_unstructured_doc = any(fname_lower.endswith(ext) for ext in [".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg"])

    if len(matching_kpi) >= 3 or (is_kpi_file and len(matching_kpi) >= 1) or (is_kpi_file and is_unstructured_doc):
        return RoutingDecision(
            dataset_domain="kpi",
            dataset_entity="kpi_cusc_master",
            route_target="legacy_kpi",
            target_silver_table="lakehouse.silver.kpi_cusc_master",
            target_quarantine_table="lakehouse.silver.kpi_cusc_quarantine",
            target_gold_table="lakehouse.gold.kpi_tong_hop_don_vi",
            business_keys=["ma_chi_tieu", "quy_danh_gia"],
            source_updated_at_field="thoi_gian_cap_nhat",
            dimension_columns=["nhom_don_vi", "quy_danh_gia", "dinh_ky_thu_thap"],
            metric_columns=["muc_dat_numeric"],
            suggested_visualizations=[
                SuggestedVisualization(
                    chart_type="bar",
                    x_axis_or_dimension="nhom_don_vi",
                    y_axis_or_metric="ty_le_hoan_thanh_phan_tram",
                    aggregation="AVG",
                    title="Tỷ lệ hoàn thành KPI theo đơn vị"
                )
            ],
            fallback_used=False,
            reasoning="Phát hiện cấu trúc cột đặc thù của KPI CUSC (legacy_kpi route)."
        )
    return None


def check_registered_api_match(columns: List[str]) -> Optional[RoutingDecision]:
    """Kiểm tra xem dữ liệu có khớp với các API dataset đã đăng ký trong registry không."""
    col_set = {to_snake_case(c) for c in columns}

    for dataset_id, cfg in DATASETS.items():
        src_fields = {to_snake_case(f) for f in cfg.source_fields}
        canonical_fields = {to_snake_case(f) for f in cfg.source_to_canonical.values()}

        # Khớp phần lớn các trường của registered dataset
        if len(col_set.intersection(src_fields)) >= len(src_fields) * 0.7 or \
           len(col_set.intersection(canonical_fields)) >= len(canonical_fields) * 0.7:
            return RoutingDecision(
                dataset_domain="education",
                dataset_entity=to_snake_case(cfg.dataset),
                route_target="registered_api",
                registered_dataset_id=dataset_id,
                target_silver_table=cfg.silver_table,
                target_quarantine_table=cfg.quarantine_table,
                target_gold_table=cfg.gold_table or f"lakehouse.gold.{to_snake_case(cfg.dataset)}_metrics",
                business_keys=list(cfg.business_key),
                source_updated_at_field=cfg.source_updated_at_field,
                dimension_columns=list(cfg.business_key),
                metric_columns=[to_snake_case(f) for f in cfg.sample_validation_fields if f not in cfg.business_key],
                suggested_visualizations=[],
                fallback_used=False,
                reasoning=f"Khớp schema của registered API dataset '{dataset_id}'."
            )
    return None


# =====================================================================
# RULE-BASED FALLBACK INFERRER (DETERMINISTIC)
# =====================================================================

def profile_with_rule_fallback(
    columns: List[str],
    sample_rows: List[Dict[str, Any]],
    suggested_name: str = "generic_dataset",
    reason: str = "Deterministic Rule-based Inferrer"
) -> RoutingDecision:
    """Bộ suy luận quy tắc dự phòng khi AI không khả dụng hoặc lỗi."""
    clean_name = to_snake_case(suggested_name) or "generic_dataset"
    if clean_name.endswith((".json", ".csv", ".tsv", ".parquet", ".xlsx", ".xls")):
        clean_name = clean_name.rsplit(".", 1)[0]

    # Chuẩn hóa tên cột
    column_mapping = {c: to_snake_case(c) for c in columns}
    sanitized_cols = list(column_mapping.values())

    # 1. Tìm Business Keys
    business_keys = []
    id_pattern = re.compile(r"(_id|id|_code|code|_ma|ma_|uuid)$", re.IGNORECASE)
    for orig, clean in column_mapping.items():
        if id_pattern.search(clean) or clean in ["id", "code", "ma"]:
            business_keys.append(clean)

    # Nếu không tìm thấy cột id rõ ràng, lấy cột đầu tiên hoặc hash toàn bộ dòng
    if not business_keys:
        if sanitized_cols:
            business_keys = [sanitized_cols[0]]
        else:
            business_keys = ["_surrogate_row_hash"]

    # 2. Tìm Timestamp field
    time_pattern = re.compile(r"(updated|modified|timestamp|created|ngay_cap_nhat|ngay_tao|thoi_gian)", re.IGNORECASE)
    source_updated_at_field = None
    for clean in sanitized_cols:
        if time_pattern.search(clean):
            source_updated_at_field = clean
            break

    # 3. Phân tách Metric vs Dimension
    metric_cols = []
    dim_cols = []

    for clean in sanitized_cols:
        # Lấy giá trị mẫu của cột này
        types_in_sample = []
        for row in sample_rows[:10]:
            # Tìm key trong row tương ứng
            val = None
            for rk, rv in row.items():
                if to_snake_case(rk) == clean:
                    val = rv
                    break
            if val is not None:
                types_in_sample.append(guess_value_type(val))

        # Nếu hầu hết là number/integer/double và không phải ID -> metric
        is_num = any(t in ["number", "integer", "double"] for t in types_in_sample)
        if is_num and clean not in business_keys and not id_pattern.search(clean):
            metric_cols.append(clean)
        else:
            dim_cols.append(clean)

    # Biểu đồ gợi ý mặc định
    suggested_charts = []
    if metric_cols and dim_cols:
        suggested_charts.append(
            SuggestedVisualization(
                chart_type="bar",
                x_axis_or_dimension=dim_cols[0],
                y_axis_or_metric=metric_cols[0],
                aggregation="SUM",
                title=f"Tổng {metric_cols[0]} theo {dim_cols[0]}"
            )
        )

    return RoutingDecision(
        dataset_domain="generic",
        dataset_entity=clean_name,
        route_target="generic_dynamic",
        target_silver_table=f"lakehouse.silver.{clean_name}",
        target_quarantine_table=f"lakehouse.silver.{clean_name}_quarantine",
        target_gold_table=f"lakehouse.gold.{clean_name}_summary",
        business_keys=business_keys,
        source_updated_at_field=source_updated_at_field,
        dimension_columns=dim_cols,
        metric_columns=metric_cols,
        suggested_visualizations=suggested_charts,
        fallback_used=True,
        reasoning=f"{reason}. Tự động suy luận Business Keys={business_keys}, Metrics={metric_cols}."
    )


def fetch_existing_silver_tables() -> List[str]:
    """Lấy danh sách các bảng Silver hiện có trong Project Nessie Catalog."""
    import urllib.request
    try:
        from env_config import NESSIE_API_URL
    except ImportError:
        NESSIE_API_URL = os.getenv("NESSIE_API_URL", "http://localhost:19120/api/v1")

    url = f"{NESSIE_API_URL.rstrip('/')}/trees/tree/main/entries?namespace=silver"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            tables = []
            for entry in data.get("entries", []):
                if entry.get("type") == "ICEBERG_TABLE":
                    elements = entry.get("name", {}).get("elements", [])
                    if len(elements) >= 2 and elements[0] == "silver":
                        tables.append(elements[1])
            return tables
    except Exception:
        return []


# =====================================================================
# GEMINI AI PROFILER (CONTROL PLANE - CATALOG-AWARE)
# =====================================================================

def profile_with_gemini(
    columns: List[str],
    sample_rows: List[Dict[str, Any]],
    suggested_name: str = "",
) -> RoutingDecision:
    """Sử dụng Gemini API để phân tích ngữ nghĩa sâu, đối chiếu Catalog hiện có và routing."""
    api_key = GEMINI_API_KEY or os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return profile_with_rule_fallback(
            columns, sample_rows, suggested_name,
            reason="GEMINI_API_KEY không được cung cấp. Chuyển sang Rule-based Inferrer"
        )

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)

        # Lấy danh sách các bảng Silver đang có trong Lakehouse
        existing_silver_tables = fetch_existing_silver_tables()
        existing_tables_str = ", ".join(f"'{t}'" for t in existing_silver_tables) if existing_silver_tables else "(Chưa có bảng nào)"

        # Chuẩn bị context gọn gàng gửi cho AI
        compact_sample = sample_rows[:5]
        prompt = f"""
Bạn là một AI Data Architect hàng đầu cho hệ thống Data Lakehouse Medallion (Apache Iceberg + Spark + Trino + Superset).
Nhiệm vụ của bạn là phân tích một tập dữ liệu đầu vào và sinh ra cấu hình định tuyến (Routing Decision) chính xác.

Tên gợi ý nguồn: "{suggested_name}"
Danh sách các cột: {columns}
Mẫu dữ liệu (5 dòng đầu tiên):
{json.dumps(compact_sample, ensure_ascii=False, indent=2, default=str)}

DANH SÁCH BẢNG SILVER ĐANG CÓ TRONG DATA LAKEHOUSE:
[{existing_tables_str}]

QUY TẮC ĐẶC BIỆT CHỐNG PHÂN MẢNH BẢNG (SEMANTIC ENTITY MATCHING):
1. Hãy kiểm tra xem tập dữ liệu mới này có cùng bản chất thực thể với một trong các bảng Silver ĐÃ CÓ ở trên hay không (ví dụ: cùng là học phí, cùng là điểm sinh viên, cùng là nhân sự... mặc dù có thể khác năm, khác mẫu file hoặc đổi tên cột).
2. NẾU KHỚP VỚI BẢNG ĐÃ CÓ:
   - Đặt `is_existing_table_match`: true.
   - `dataset_entity`: Giữ nguyên tên entity của bảng cũ đó.
   - `target_silver_table`: Giữ nguyên bảng cũ "lakehouse.silver.<bảng_cũ>".
   - `column_mapping`: Bản đồ ánh xạ các cột có cùng ý nghĩa từ nguồn sang cột bảng cũ (ví dụ: {{"student_code": "student_id", "full_name": "student_name"}}).
   - `new_columns`: Danh sách các cột mới toanh xuất hiện lần đầu chưa có trong bảng cũ.
3. NẾU LÀ THỰC THỂ MỚI HOÀN TOÀN (chưa có bảng nào tương đương):
   - Đặt `is_existing_table_match`: false.
   - `column_mapping`: {{}}.
   - `new_columns`: [].
   - Đặt tên entity mới chuẩn snake_case tiếng Anh.

YÊU CẦU ĐỊNH TUYẾN:
- `dataset_domain`: Lĩnh vực (education, finance, hr, admissions, operations, generic).
- `business_keys`: Khóa chính nghiệp vụ duy nhất của bản ghi.
- `source_updated_at_field`: Cột timestamp cập nhật gần nhất (nếu có).
- `dimension_columns`: Các cột phân loại/danh mục (Dimension).
- `metric_columns`: Các cột số liệu định lượng có thể SUM/AVG/COUNT (Metric).
- `suggested_visualizations`: 2-3 biểu đồ Superset hữu ích.

Hãy phản hồi DUY NHẤT một chuỗi JSON hợp lệ khớp với cấu trúc Pydantic sau (không bọc trong markdown codeblock nếu có thể, hoặc dùng json format):
{{
  "dataset_domain": "...",
  "dataset_entity": "...",
  "route_target": "generic_dynamic",
  "target_silver_table": "lakehouse.silver.<dataset_entity>",
  "target_quarantine_table": "lakehouse.silver.<dataset_entity>_quarantine",
  "target_gold_table": "lakehouse.gold.<dataset_entity>_summary",
  "business_keys": ["..."],
  "source_updated_at_field": "...",
  "dimension_columns": ["..."],
  "metric_columns": ["..."],
  "suggested_visualizations": [
    {{
      "chart_type": "bar",
      "x_axis_or_dimension": "...",
      "y_axis_or_metric": "...",
      "aggregation": "SUM",
      "title": "..."
    }}
  ],
  "is_existing_table_match": false,
  "column_mapping": {{}},
  "new_columns": [],
  "fallback_used": false,
  "reasoning": "..."
}}
"""
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.1,
            ),
        )

        response_text = response.text.strip()
        if response_text.startswith("```"):
            response_text = re.sub(r"^```(?:json)?\n", "", response_text)
            response_text = re.sub(r"\n```$", "", response_text)

        data = json.loads(response_text)
        data["fallback_used"] = False
        decision = RoutingDecision(**data)
        return decision

    except Exception as exc:
        print(f"⚠️ [ai_dataset_router] Lỗi khi gọi Gemini AI: {exc}. Tự động kích hoạt Fallback!")
        return profile_with_rule_fallback(
            columns, sample_rows, suggested_name,
            reason=f"Gemini API gặp sự cố ({exc})"
        )


# =====================================================================
# MAIN ENTRYPOINT: ROUTE FROM DIFFERENT SOURCES
# =====================================================================

def route_dataset(
    sample_records: List[Dict[str, Any]],
    source_name: str = "dataset",
) -> RoutingDecision:
    """Hàm trung tâm: Phân tích mẫu dữ liệu và trả về RoutingDecision."""
    if not sample_records:
        raise ValueError("sample_records không được để trống")

    # Lấy danh sách tên cột từ bản ghi mẫu đầu tiên
    columns = list(sample_records[0].keys())

    # Bước 1: Kiểm tra Legacy KPI
    legacy_decision = check_legacy_kpi_match(columns, source_name)
    if legacy_decision:
        return legacy_decision

    # Bước 2: Kiểm tra Registered API
    registered_decision = check_registered_api_match(columns)
    if registered_decision:
        return registered_decision

    # Bước 3: Phân tích bằng Gemini AI (hoặc Fallback nếu không có key/lỗi)
    return profile_with_gemini(columns, sample_records, source_name)


def route_from_json_string(json_str: str, source_name: str = "api_payload") -> RoutingDecision:
    """Định tuyến trực tiếp từ chuỗi JSON raw (từ REST API hoặc UI)."""
    parsed = json.loads(json_str)
    if isinstance(parsed, dict):
        # Có thể dữ liệu nằm trong key 'data' hoặc 'items'
        for k in ["data", "items", "records", "results"]:
            if k in parsed and isinstance(parsed[k], list) and parsed[k]:
                parsed = parsed[k]
                break
        if isinstance(parsed, dict):
            parsed = [parsed]
    if not isinstance(parsed, list):
        raise ValueError("JSON không ở dạng danh sách các bản ghi (list of dicts)")

    return route_dataset(parsed, source_name)


def profile_and_extract_document_with_gemini(file_path: Path) -> RoutingDecision:
    """Sử dụng Gemini Multimodal để đọc tài liệu (PDF, Word, Ảnh), xác định loại tài liệu
    và bóc tách bảng thành JSON nếu không phải KPI CUSC."""
    filename = file_path.name
    suffix = file_path.suffix.lower()

    # Kiểm tra nhanh: Nếu tên file chứa từ khóa rõ ràng của KPI CUSC
    fname_lower = filename.lower()
    if any(kw in fname_lower for kw in ["kpi_cusc", "cusc_kpi", "danh_gia_muc_tieu_cusc"]):
        return RoutingDecision(
            dataset_domain="kpi",
            dataset_entity="kpi_cusc_master",
            route_target="legacy_kpi",
            target_silver_table="lakehouse.silver.kpi_cusc_master",
            target_quarantine_table="lakehouse.silver.kpi_cusc_quarantine",
            target_gold_table="lakehouse.gold.kpi_tong_hop_don_vi",
            business_keys=["ma_chi_tieu", "quy_danh_gia"],
            source_updated_at_field="thoi_gian_cap_nhat",
            dimension_columns=["nhom_don_vi", "quy_danh_gia"],
            metric_columns=["muc_dat_numeric"],
            suggested_visualizations=[],
            fallback_used=False,
            reasoning=f"Tài liệu '{filename}' được nhận diện là báo cáo KPI CUSC, định tuyến sang legacy_kpi."
        )

    api_key = GEMINI_API_KEY or os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return RoutingDecision(
            dataset_domain="unstructured",
            dataset_entity="kpi_cusc_master",
            route_target="legacy_kpi",
            target_silver_table="lakehouse.silver.kpi_cusc_master",
            target_quarantine_table="lakehouse.silver.kpi_cusc_quarantine",
            target_gold_table="lakehouse.gold.kpi_tong_hop_don_vi",
            business_keys=["ma_chi_tieu", "quy_danh_gia"],
            fallback_used=True,
            reasoning="Thiếu GEMINI_API_KEY để phân tích tài liệu phi cấu trúc."
        )

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        with open(file_path, "rb") as f:
            file_bytes = f.read()

        mime_map = {
            ".pdf": "application/pdf",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png"
        }
        mime_type = mime_map.get(suffix, "application/pdf")
        file_part = types.Part.from_bytes(data=file_bytes, mime_type=mime_type)

        prompt = """Bạn là một chuyên gia AI Data Architect cho Data Lakehouse.
Hãy đọc kỹ tài liệu đính kèm này và thực hiện các yêu cầu:
1. Xác định đây có phải là báo cáo đánh giá KPI/mục tiêu của CUSC không? (Các bảng có Mã chỉ tiêu, Mức đăng ký, Mức đạt, Kết quả hệ thống).
   - Nếu ĐÚNG là KPI CUSC: Đặt "is_kpi_cusc": true.
   - Nếu KHÔNG PHẢI KPI CUSC (ví dụ: Quyết định khen thưởng, Bảng điểm sinh viên, Danh sách học phí, Bảng lương, v.v.): Đặt "is_kpi_cusc": false.
2. Nếu "is_kpi_cusc": false:
   - Trích xuất toàn bộ dữ liệu bảng trong tài liệu thành danh sách các dòng dữ liệu.
   - Chuẩn hóa tên các cột thành snake_case không dấu (ví dụ: stt, ma_sv, ho_va_ten, ngay_sinh, lop, don_vi, so_tcdk, diem_tb, diem_tbrl, xep_loai, nganh, chuyen_nganh).
   - Xác định:
     * dataset_domain: education, finance, hr, generic...
     * dataset_entity: Tên thực thể chuẩn snake_case tiếng Anh (ví dụ: student_awards_k48, student_scores, tuition_payments...)
     * business_keys: Danh sách cột khóa chính định danh (ví dụ: ["ma_sv"])
     * dimension_columns: Các cột phân loại (ví dụ: ["lop", "xep_loai", "nganh"])
     * metric_columns: Các cột số liệu định lượng (ví dụ: ["diem_tb", "diem_tbrl", "so_tcdk"])
     * reasoning: Tóm tắt ngắn gọn nội dung tài liệu.

Trả về duy nhất định dạng JSON:
{
  "is_kpi_cusc": false,
  "dataset_domain": "education",
  "dataset_entity": "student_awards_k48",
  "columns": ["stt", "ma_sv", "ho_va_ten", "ngay_sinh", "lop", "don_vi", "so_tcdk", "diem_tb", "diem_tbrl", "xep_loai", "nganh"],
  "business_keys": ["ma_sv"],
  "dimension_columns": ["lop", "don_vi", "xep_loai", "nganh"],
  "metric_columns": ["so_tcdk", "diem_tb", "diem_tbrl"],
  "reasoning": "...",
  "records": [ ...toàn bộ các dòng dữ liệu dạng dict hoặc array các giá trị... ]
}
"""

        # Thử lần lượt các mô hình Gemini với cơ chế exponential backoff
        candidate_models = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash"]
        response = None
        last_error = None

        for model_name in candidate_models:
            for attempt in range(3):
                try:
                    response = client.models.generate_content(
                        model=model_name,
                        contents=[file_part, prompt],
                        config=types.GenerateContentConfig(
                            response_mime_type="application/json",
                            temperature=0.0
                        )
                    )
                    if response and response.text:
                        break
                except Exception as api_err:
                    last_error = api_err
                    err_str = str(api_err).lower()
                    if any(k in err_str for k in ["503", "429", "unavailable", "high demand", "resource_exhausted", "quota"]):
                        wait_sec = 3 * (2 ** attempt)
                        print(f"⚠️ Gemini API tạm thời quá tải ({model_name}). Đang thử lại sau {wait_sec}s (Lần {attempt+1}/3)...")
                        time.sleep(wait_sec)
                        continue
                    else:
                        break
            if response and response.text:
                break

        if not response or not response.text:
            raise RuntimeError(f"Tất cả các mô hình Gemini đều quá tải hoặc không khả dụng: {last_error}")

        res_data = json.loads(response.text)

        if res_data.get("is_kpi_cusc", False):
            return RoutingDecision(
                dataset_domain="kpi",
                dataset_entity="kpi_cusc_master",
                route_target="legacy_kpi",
                target_silver_table="lakehouse.silver.kpi_cusc_master",
                target_quarantine_table="lakehouse.silver.kpi_cusc_quarantine",
                target_gold_table="lakehouse.gold.kpi_tong_hop_don_vi",
                business_keys=["ma_chi_tieu", "quy_danh_gia"],
                source_updated_at_field="thoi_gian_cap_nhat",
                dimension_columns=["nhom_don_vi", "quy_danh_gia"],
                metric_columns=["muc_dat_numeric"],
                suggested_visualizations=[],
                fallback_used=False,
                reasoning=f"Gemini phân tích nội dung '{filename}': xác nhận là báo cáo KPI CUSC."
            )

        raw_records = res_data.get("records", [])
        columns = [to_snake_case(c) for c in res_data.get("columns", [])]

        if not raw_records:
            return RoutingDecision(
                dataset_domain="unstructured",
                dataset_entity=to_snake_case(filename.rsplit(".", 1)[0]),
                route_target="legacy_kpi",
                target_silver_table="lakehouse.silver.kpi_cusc_master",
                target_quarantine_table="lakehouse.silver.kpi_cusc_quarantine",
                target_gold_table="lakehouse.gold.kpi_tong_hop_don_vi",
                business_keys=["ma_chi_tieu", "quy_danh_gia"],
                fallback_used=True,
                reasoning=f"Không tìm thấy bảng dữ liệu trong tài liệu '{filename}'."
            )

        # Chuẩn hóa raw_records thành list of dicts
        normalized_records = []
        for item in raw_records:
            if isinstance(item, dict):
                normalized_records.append({to_snake_case(k): v for k, v in item.items()})
            elif isinstance(item, list) and columns:
                normalized_records.append(dict(zip(columns, item)))

        # Lưu records ra file JSON để Spark Dynamic Processor đọc trực tiếp
        extracted_dir = Path("/opt/airflow/spark/.extracted_tables")
        try:
            extracted_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            extracted_dir = Path("./lakehouse/spark/.extracted_tables")
            extracted_dir.mkdir(parents=True, exist_ok=True)

        entity = to_snake_case(res_data.get("dataset_entity", filename.rsplit(".", 1)[0])) or "extracted_document"
        json_file_path = str(extracted_dir / f"{entity}_{int(time.time())}.json")
        with open(json_file_path, "w", encoding="utf-8") as jf:
            json.dump(normalized_records, jf, ensure_ascii=False, indent=2)

        print(f"📄 [Document Extractor] Đã bóc tách thành công {len(normalized_records)} dòng từ PDF vào '{json_file_path}'")

        dims = [to_snake_case(d) for d in res_data.get("dimension_columns", [])]
        mets = [to_snake_case(m) for m in res_data.get("metric_columns", [])]
        b_keys = [to_snake_case(k) for k in res_data.get("business_keys", [])]
        if not b_keys and normalized_records:
            b_keys = [list(normalized_records[0].keys())[0]]

        charts = []
        if dims and mets:
            charts.append(
                SuggestedVisualization(
                    chart_type="bar",
                    x_axis_or_dimension=dims[0],
                    y_axis_or_metric=mets[0],
                    aggregation="AVG",
                    title=f"Trung bình {mets[0]} theo {dims[0]}"
                )
            )

        return RoutingDecision(
            dataset_domain=res_data.get("dataset_domain", "education"),
            dataset_entity=entity,
            route_target="generic_dynamic",
            target_silver_table=f"lakehouse.silver.{entity}",
            target_quarantine_table=f"lakehouse.silver.{entity}_quarantine",
            target_gold_table=f"lakehouse.gold.{entity}_summary",
            business_keys=b_keys,
            source_updated_at_field=None,
            dimension_columns=dims,
            metric_columns=mets,
            suggested_visualizations=charts,
            extracted_json_path=json_file_path,
            fallback_used=False,
            reasoning=f"Gemini đọc tài liệu '{filename}': bóc tách thành công {len(normalized_records)} dòng dữ liệu bảng, định tuyến sang generic_dynamic."
        )

    except Exception as exc:
        print(f"❌ [Document Extractor Error] Lỗi khi bóc tách tài liệu '{filename}': {exc}")
        raise RuntimeError(
            f"Không thể bóc tách bảng từ tài liệu '{filename}' qua Gemini AI ({exc}). Vui lòng thử lại sau vài giây!"
        )


def route_from_file_path(file_path: Union[str, Path]) -> RoutingDecision:
    """Định tuyến từ đường dẫn file trên đĩa (CSV, JSON, PDF, DOCX, Ảnh)."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    filename = path.name
    suffix = path.suffix.lower()

    if suffix == ".json":
        with open(path, "r", encoding="utf-8") as f:
            return route_from_json_string(f.read(), source_name=filename)

    elif suffix in [".csv", ".tsv"]:
        delimiter = "\t" if suffix == ".tsv" else ","
        records = []
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for i, row in enumerate(reader):
                records.append(row)
                if i >= 15:  # Lấy 15 dòng mẫu
                    break
        return route_dataset(records, source_name=filename)

    elif suffix in [".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg"]:
        # Tự động đọc tài liệu bằng Gemini Multimodal để xác định loại tài liệu
        return profile_and_extract_document_with_gemini(path)

    else:
        # Nếu là file parquet hoặc file khác
        dummy_cols = ["file_content", "checksum"]
        return profile_with_rule_fallback(dummy_cols, [], suggested_name=filename)


# =====================================================================
# CLI RUNNER FOR TESTING & AIRFLOW INTEGRATION
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="AI Semantic Profiler & Router for Lakehouse")
    parser.add_argument("--file", "-f", help="Đường dẫn tới file cần phân tích (JSON, CSV)")
    parser.add_argument("--json-payload", "-p", help="Chuỗi JSON payload")
    parser.add_argument("--output-json", "-o", help="Đường dẫn file ghi kết quả RoutingDecision JSON")
    parser.add_argument("--test-samples", action="store_true", help="Chạy kiểm thử với các mẫu dữ liệu mô phỏng")
    args = parser.parse_args()

    if args.test_samples:
        print("\n=======================================================")
        print("  KIỂM THỬ AI DATASET ROUTER VỚI CÁC MẪU DỮ LIỆU")
        print("=======================================================\n")

        # Sample 1: Dữ liệu học phí sinh viên (Dataset mới bất kỳ)
        tuition_sample = [
            {"student_id": "SV001", "student_name": "Nguyễn Văn A", "faculty": "CNTT", "academic_year": "2025-2026", "semester": 1, "amount_paid": 8500000, "paid_at": "2026-01-15 08:30:00"},
            {"student_id": "SV002", "student_name": "Trần Thị B", "faculty": "Kinh tế", "academic_year": "2025-2026", "semester": 1, "amount_paid": 7800000, "paid_at": "2026-01-16 09:15:00"},
            {"student_id": "SV003", "student_name": "Lê Văn C", "faculty": "Luật", "academic_year": "2025-2026", "semester": 1, "amount_paid": 6500000, "paid_at": "2026-01-17 14:00:00"},
        ]
        decision1 = route_dataset(tuition_sample, source_name="hoc_phi_sinh_vien.json")
        print(">>> Mẫu 1 (Học phí sinh viên - Dữ liệu mới):")
        print(decision1.model_dump_json(indent=2))
        print("-" * 60)

        # Sample 2: Dữ liệu KPI Legacy (Word/Excel xuất từ phòng ban)
        kpi_sample = [
            {"ma_chi_tieu": "ĐT.01", "nhom_don_vi": "ĐT", "quy_danh_gia": "Q1/2026", "muc_dang_ky": "100%", "muc_dat": "95%", "ket_qua_he_thong": "ĐẠT"},
            {"ma_chi_tieu": "PM.02", "nhom_don_vi": "PM", "quy_danh_gia": "Q1/2026", "muc_dang_ky": "10", "muc_dat": "8", "ket_qua_he_thong": "KHÔNG ĐẠT"},
        ]
        decision2 = route_dataset(kpi_sample, source_name="kpi_quy_1_2026.xlsx")
        print(">>> Mẫu 2 (KPI CUSC Legacy):")
        print(decision2.model_dump_json(indent=2))
        print("-" * 60)

        # Sample 3: Dữ liệu Registered API (Tiến độ giảng dạy)
        teaching_sample = [
            {"record_id": "rec_01", "unit_code": "PM", "course_section_code": "HP001", "academic_year": "2025-2026", "semester": 1, "progress_percent": 80.5, "updated_at": "2026-03-01 10:00:00"},
        ]
        decision3 = route_dataset(teaching_sample, source_name="api_teaching_progress")
        print(">>> Mẫu 3 (Registered API - Teaching Progress):")
        print(decision3.model_dump_json(indent=2))
        print("-" * 60)
        return

    decision = None
    if args.file:
        decision = route_from_file_path(args.file)
    elif args.json_payload:
        decision = route_from_json_string(args.json_payload)
    else:
        parser.print_help()
        sys.exit(1)

    output_str = decision.model_dump_json(indent=2)
    print(output_str)

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            f.write(output_str)
        print(f"✅ Đã lưu kết quả định tuyến vào: {args.output_json}")


if __name__ == "__main__":
    main()
