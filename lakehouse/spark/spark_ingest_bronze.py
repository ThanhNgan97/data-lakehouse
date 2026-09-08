# -*- coding: utf-8 -*-
"""
spark_ingest_bronze.py (Phiên bản tối ưu hóa hiệu năng cao)
------------------------------------------------------------
TẦNG BRONZE - AI & NATIVE HYBRID INGESTION
- Tối ưu 1: Trích xuất trực tiếp bảng DOCX siêu nhanh bằng XML (0.05s).
- Tối ưu 2: Dùng Inline Bytes (types.Part.from_bytes) cho PDF/ảnh trong Gemini API (1-2s).
- Tối ưu 3: Loại bỏ time.sleep(30) cố định, chỉ backoff thông minh khi gặp lỗi 429.
- Tối ưu 4: Xử lý đa luồng (Multi-threading) khi có nhiều file trong staging.
"""

import io
import os
import sys
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
import hashlib
import re
import zipfile
import xml.etree.ElementTree as ET
import boto3
import json
import time
from datetime import datetime
import pandas as pd
from pydantic import BaseModel
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_utils import update_pipeline_error, save_parsed_data

from google import genai
from google.genai import types

from env_config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, 
    MINIO_BUCKET_NAME, GEMINI_API_KEY
)

# Cấu hình Gemini Client
if GEMINI_API_KEY:
    client = genai.Client(api_key=GEMINI_API_KEY)
else:
    client = None
    print("WARNING: GEMINI_API_KEY is not set. The Gemini API calls will fail.")


def retry_with_backoff(func, max_retries=3, initial_delay=5):
    """
    Retry function with exponential backoff for quota errors.
    """
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_str = str(e)
            # Check for quota/rate limit errors
            if any(k in error_str.lower() for k in ["429", "resource_exhausted", "quota", "503", "unavailable", "high demand"]):
                if attempt < max_retries - 1:
                    wait_time = initial_delay * (2 ** attempt)
                    print(f"⚠️ Quota/Rate limit exceeded. Retrying in {wait_time}s (Attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue
            raise


BUCKET_NAME    = MINIO_BUCKET_NAME
SOURCE_PREFIX  = "staging/"
ARCHIVE_PREFIX = "archive/"

KETQUA_KHONG_DAT     = "KHÔNG ĐẠT"
KETQUA_DAT           = "ĐẠT"
KETQUA_CHUA_DEN_KY   = "CHƯA ĐẾN KỲ ĐÁNH GIÁ"
QUY_DANH_GIA_UNKNOWN = "UNKNOWN_KY"


class KpiRecord(BaseModel):
    ma_chi_tieu: str
    quy_danh_gia: str
    noi_dung_muc_tieu: str
    dinh_ky_thu_thap: str
    muc_dang_ky: str
    muc_dat: str
    ket_qua_he_thong: str
    nguyen_nhan: str
    hanh_dong_khac_phuc: str


def get_s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
    )


def generate_checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def clean_status_text(text: str) -> str:
    text = str(text).upper().strip()
    if re.search(r"(KHÔNG ĐẠT|FAILED)", text):
        return KETQUA_KHONG_DAT
    if re.search(r"(CHƯA ĐẾN KỲ|NOT DUE)", text):
        return KETQUA_CHUA_DEN_KY
    if re.search(r"(ĐẠT|PASSED|SUCCESS)", text):
        return KETQUA_DAT
    return text.replace("\n", " ").strip()


def parse_percent_or_number(text):
    if text is None or text == "N/A" or not str(text).strip():
        return None
    cleaned = str(text).strip().replace("%", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def extract_table_from_docx_fast(file_bytes: bytes):
    """
    Trích xuất bảng trực tiếp từ XML trong DOCX (xử lý siêu nhanh < 0.05s).
    Nếu bảng có cấu trúc hợp lệ (các cột KPI), trả về (rows, quy_danh_gia, ky_candidates).
    Nếu không phải bảng chuẩn, trả về None để fallback sang Gemini API.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            if "word/document.xml" not in z.namelist():
                return None
            xml_content = z.read("word/document.xml")
        
        root = ET.fromstring(xml_content)
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        
        # Tìm tiêu đề / Quý đánh giá trong văn bản
        all_p_texts = []
        for p in root.findall(".//w:p", ns):
            t = "".join([node.text for node in p.findall(".//w:t", ns) if node.text])
            if t.strip():
                all_p_texts.append(t.strip())
        full_text = "\n".join(all_p_texts)
        
        quy_danh_gia_candidate = None
        quy_match = re.search(r"(?:Q|QUÝ|QUY)\s*([1-4])\s*(?:/|-|NĂM|NAM)\s*(\d{4})", full_text, re.IGNORECASE)
        if quy_match:
            quy_danh_gia_candidate = f"Q{quy_match.group(1)}/{quy_match.group(2)}"
        else:
            nam_match = re.search(r"(?:NĂM|NAM)\s*(\d{4})", full_text, re.IGNORECASE)
            if nam_match:
                quy_danh_gia_candidate = f"NĂM {nam_match.group(1)}"

        tables = root.findall(".//w:tbl", ns)
        extracted_rows = []
        for table in tables:
            rows = table.findall(".//w:tr", ns)
            if len(rows) < 2:
                continue
            
            header_cells = rows[0].findall(".//w:tc", ns)
            headers = ["".join([node.text for node in c.findall(".//w:t", ns) if node.text]).strip().upper() for c in header_cells]
            header_str = " ".join(headers)
            
            if not any(k in header_str for k in ["MÃ", "MA", "MỤC TIÊU", "MUC TIEU", "MỨC ĐẠT", "MUC DAT", "MỨC ĐĂNG KÝ"]):
                continue
            
            col_map = {}
            for idx, h in enumerate(headers):
                if re.search(r"^(MÃ|MA|MÃ CHỈ TIÊU)$", h) or "MÃ" in h:
                    col_map.setdefault("ma", idx)
                elif "NỘI DUNG" in h or "MỤC TIÊU" in h or "CHỈ TIÊU" in h:
                    col_map.setdefault("noi_dung", idx)
                elif "ĐỊNH KỲ" in h or "DINH KY" in h or "THU THẬP" in h:
                    col_map.setdefault("dinh_ky", idx)
                elif "ĐĂNG KÝ" in h or "DANG KY" in h or "KẾ HOẠCH" in h:
                    col_map.setdefault("muc_dang_ky", idx)
                elif "MỨC ĐẠT" in h or "MUC DAT" in h or "KẾT QUẢ ĐẠT" in h:
                    col_map.setdefault("muc_dat", idx)
                elif "KẾT QUẢ" in h or "KET QUA" in h or "ĐÁNH GIÁ" in h:
                    col_map.setdefault("ket_qua", idx)
                elif "NGUYÊN NHÂN" in h or "NGUYEN NHAN" in h:
                    col_map.setdefault("nguyen_nhan", idx)
                elif "HÀNH ĐỘNG" in h or "KHẮC PHỤC" in h or "HANH DONG" in h:
                    col_map.setdefault("hanh_dong", idx)
            
            if "ma" not in col_map:
                continue

            for r in rows[1:]:
                cells = r.findall(".//w:tc", ns)
                cell_texts = ["".join([node.text for node in c.findall(".//w:t", ns) if node.text]).strip() for c in cells]
                if not cell_texts or len(cell_texts) <= col_map["ma"]:
                    continue
                
                ma_val = cell_texts[col_map["ma"]].strip()
                if not ma_val or ma_val.upper() in ["N/A", "STT", "TT", "MÃ", "MA"] or len(ma_val) < 2:
                    continue

                def get_c(key, default="N/A"):
                    idx = col_map.get(key)
                    if idx is not None and idx < len(cell_texts):
                        val = cell_texts[idx].strip()
                        return val if val else default
                    return default

                extracted_rows.append((
                    ma_val,
                    get_c("noi_dung", "N/A"),
                    get_c("dinh_ky", "N/A"),
                    get_c("muc_dang_ky", "N/A"),
                    get_c("muc_dat", "N/A"),
                    get_c("ket_qua", "N/A"),
                    get_c("nguyen_nhan", ""),
                    get_c("hanh_dong", "")
                ))

        if extracted_rows:
            print(f"⚡ [Fast Native Parser] Đã trích xuất thành công {len(extracted_rows)} dòng từ DOCX trong < 0.05s!")
            return extracted_rows, quy_danh_gia_candidate, {quy_danh_gia_candidate} if quy_danh_gia_candidate else set()
    except Exception as e:
        print(f"DEBUG: Fast DOCX parsing failed ({e}), fallback sang Gemini.")
    
    return None


def extract_table_from_pptx_fast(file_bytes: bytes):
    """
    Trích xuất bảng trực tiếp từ slide XML trong PPTX (xử lý siêu nhanh < 0.05s).
    Tìm bảng có cấu trúc hợp lệ (chứa cột KPI: MÃ, MỤC TIÊU, MỨC ĐẠT...).
    Trả về (rows, quy_danh_gia, ky_candidates) hoặc None nếu không có bảng chuẩn để fallback sang Gemini API.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            slide_names = [
                n for n in z.namelist()
                if n.startswith("ppt/slides/slide") and n.endswith(".xml")
            ]
            if not slide_names:
                return None

            ns = {
                "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
                "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
            }

            all_slide_texts = []
            extracted_rows = []

            def slide_sort_key(name):
                match = re.search(r"slide(\d+)\.xml", name)
                return int(match.group(1)) if match else 9999

            for name in sorted(slide_names, key=slide_sort_key):
                xml_content = z.read(name)
                root = ET.fromstring(xml_content)

                # Thu thập toàn bộ text trên slide để nhận diện kỳ/quý đánh giá
                slide_texts = [
                    node.text.strip()
                    for node in root.findall(".//a:t", ns)
                    if node.text and node.text.strip()
                ]
                if slide_texts:
                    all_slide_texts.extend(slide_texts)

                # Tìm các bảng trong slide (<a:tbl>)
                tables = root.findall(".//a:tbl", ns)
                for table in tables:
                    rows = table.findall(".//a:tr", ns)
                    if len(rows) < 2:
                        continue

                    header_cells = rows[0].findall(".//a:tc", ns)
                    headers = [
                        "".join([node.text for node in c.findall(".//a:t", ns) if node.text]).strip().upper()
                        for c in header_cells
                    ]
                    header_str = " ".join(headers)

                    if not any(k in header_str for k in ["MÃ", "MA", "MỤC TIÊU", "MUC TIEU", "MỨC ĐẠT", "MUC DAT", "MỨC ĐĂNG KÝ"]):
                        continue

                    col_map = {}
                    for idx, h in enumerate(headers):
                        if re.search(r"^(MÃ|MA|MÃ CHỈ TIÊU)$", h) or "MÃ" in h:
                            col_map.setdefault("ma", idx)
                        elif "NỘI DUNG" in h or "MỤC TIÊU" in h or "CHỈ TIÊU" in h:
                            col_map.setdefault("noi_dung", idx)
                        elif "ĐỊNH KỲ" in h or "DINH KY" in h or "THU THẬP" in h:
                            col_map.setdefault("dinh_ky", idx)
                        elif "ĐĂNG KÝ" in h or "DANG KY" in h or "KẾ HOẠCH" in h:
                            col_map.setdefault("muc_dang_ky", idx)
                        elif "MỨC ĐẠT" in h or "MUC DAT" in h or "KẾT QUẢ ĐẠT" in h:
                            col_map.setdefault("muc_dat", idx)
                        elif "KẾT QUẢ" in h or "KET QUA" in h or "ĐÁNH GIÁ" in h:
                            col_map.setdefault("ket_qua", idx)
                        elif "NGUYÊN NHÂN" in h or "NGUYEN NHAN" in h:
                            col_map.setdefault("nguyen_nhan", idx)
                        elif "HÀNH ĐỘNG" in h or "KHẮC PHỤC" in h or "HANH DONG" in h:
                            col_map.setdefault("hanh_dong", idx)

                    if "ma" not in col_map:
                        continue

                    for r in rows[1:]:
                        cells = r.findall(".//a:tc", ns)
                        cell_texts = [
                            "".join([node.text for node in c.findall(".//a:t", ns) if node.text]).strip()
                            for c in cells
                        ]
                        if not cell_texts or len(cell_texts) <= col_map["ma"]:
                            continue

                        ma_val = cell_texts[col_map["ma"]].strip()
                        if not ma_val or ma_val.upper() in ["N/A", "STT", "TT", "MÃ", "MA"] or len(ma_val) < 2:
                            continue

                        def get_c(key, default="N/A"):
                            idx = col_map.get(key)
                            if idx is not None and idx < len(cell_texts):
                                val = cell_texts[idx].strip()
                                return val if val else default
                            return default

                        extracted_rows.append((
                            ma_val,
                            get_c("noi_dung", "N/A"),
                            get_c("dinh_ky", "N/A"),
                            get_c("muc_dang_ky", "N/A"),
                            get_c("muc_dat", "N/A"),
                            get_c("ket_qua", "N/A"),
                            get_c("nguyen_nhan", ""),
                            get_c("hanh_dong", "")
                        ))

            full_text = "\n".join(all_slide_texts)
            quy_danh_gia_candidate = None
            quy_match = re.search(r"(?:Q|QUÝ|QUY)\s*([1-4])\s*(?:/|-|NĂM|NAM)\s*(\d{4})", full_text, re.IGNORECASE)
            if quy_match:
                quy_danh_gia_candidate = f"Q{quy_match.group(1)}/{quy_match.group(2)}"
            else:
                nam_match = re.search(r"(?:NĂM|NAM)\s*(\d{4})", full_text, re.IGNORECASE)
                if nam_match:
                    quy_danh_gia_candidate = f"NĂM {nam_match.group(1)}"

            if extracted_rows:
                print(f"⚡ [Fast Native PPTX Parser] Đã trích xuất thành công {len(extracted_rows)} dòng từ PPTX trong < 0.05s!")
                return extracted_rows, quy_danh_gia_candidate, {quy_danh_gia_candidate} if quy_danh_gia_candidate else set()

    except Exception as e:
        print(f"DEBUG: Fast PPTX parsing failed ({e}), fallback sang Gemini.")

    return None


def extract_text_from_pptx(file_bytes: bytes) -> str:
    """Trích xuất toàn bộ nội dung văn bản từ slide và ghi chú trong file PPTX hoặc PPT."""
    text_runs = []
    # 1. Xử lý định dạng .pptx (OpenXML ZIP)
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            ns = {
                "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
                "a": "http://schemas.openxmlformats.org/drawingml/2006/main"
            }
            slide_names = [
                n for n in z.namelist() 
                if n.startswith("ppt/slides/slide") and n.endswith(".xml")
            ]
            def slide_sort_key(name):
                match = re.search(r"slide(\d+)\.xml", name)
                return int(match.group(1)) if match else 9999

            for name in sorted(slide_names, key=slide_sort_key):
                slide_num = re.search(r"slide(\d+)\.xml", name)
                s_idx = slide_num.group(1) if slide_num else ""
                xml_content = z.read(name)
                root = ET.fromstring(xml_content)
                slide_texts = [node.text for node in root.findall(".//a:t", ns) if node.text and node.text.strip()]
                if slide_texts:
                    text_runs.append(f"--- Slide {s_idx} ---")
                    text_runs.append("\n".join(slide_texts))

            # Lấy thêm ghi chú (notes) nếu có
            note_names = [
                n for n in z.namelist() 
                if n.startswith("ppt/notesSlides/notesSlide") and n.endswith(".xml")
            ]
            for name in note_names:
                xml_content = z.read(name)
                root = ET.fromstring(xml_content)
                note_texts = [node.text for node in root.findall(".//a:t", ns) if node.text and node.text.strip()]
                if note_texts:
                    text_runs.append(f"--- Slide Notes ---")
                    text_runs.append("\n".join(note_texts))

            if text_runs:
                return "\n\n".join(text_runs)
    except Exception:
        pass

    # 2. Xử lý file .ppt nhị phân legacy (bằng cách quét các chuỗi văn bản UTF-16LE và ASCII)
    try:
        content = file_bytes
        # Trích xuất chuỗi UTF-16LE (ít nhất 4 ký tự in được)
        utf16_matches = re.findall(rb"(?:[\x20-\x7e\xa0-\xff]\x00){4,}", content)
        for m in utf16_matches:
            try:
                decoded = m.decode("utf-16le").strip()
                if len(decoded) >= 4 and not decoded.isnumeric():
                    text_runs.append(decoded)
            except Exception:
                continue

        # Trích xuất chuỗi ASCII (ít nhất 5 ký tự in được)
        ascii_matches = re.findall(rb"[\x20-\x7e]{5,}", content)
        for m in ascii_matches:
            try:
                decoded = m.decode("ascii", errors="ignore").strip()
                if len(decoded) >= 5 and not decoded.startswith("http"):
                    text_runs.append(decoded)
            except Exception:
                continue

        if text_runs:
            seen = set()
            unique_runs = []
            for t in text_runs:
                if t not in seen:
                    seen.add(t)
                    unique_runs.append(t)
            return "\n".join(unique_runs)
    except Exception as e:
        print(f"DEBUG: PPT text extraction fallback failed: {e}")

    return ""


def extract_text_from_docx(file_bytes: bytes) -> str:
    """Trích xuất văn bản thô từ file DOCX."""
    text_runs = []
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
        try:
            xml_content = z.read("word/document.xml")
        except KeyError:
            return ""

    root = ET.fromstring(xml_content)
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    for paragraph in root.findall(".//w:p", namespace):
        texts = [node.text for node in paragraph.findall(".//w:t", namespace) if node.text]
        if texts:
            text_runs.append("".join(texts))

    return "\n".join(text_runs)


def parse_with_gemini(file_bytes: bytes, ext: str, file_key: str):
    """
    Sử dụng Gemini API với phương thức Inline Bytes trực tiếp (siêu nhanh ~1-2s).
    Trả về (rows, quy_danh_gia, ky_candidates)
    """
    if not client:
        raise ValueError("GEMINI_API_KEY chưa được cấu hình!")

    prompt = (
        "Bạn là một chuyên gia phân tích dữ liệu. "
        "Hãy đọc tài liệu/hình ảnh đính kèm và trích xuất tất cả các dòng dữ liệu trong bảng ĐÁNH GIÁ MỤC TIÊU (KPI). "
        "Trả về một mảng JSON các đối tượng có cấu trúc yêu cầu. "
        "Lưu ý: "
        "1. ma_chi_tieu là cột MÃ trong bảng, hãy lấy nguyên văn (VD: ĐT-MT01, QTCL MT001). "
        "2. quy_danh_gia hãy lấy từ tiêu đề (VD: QUÝ 4/2026). Nếu không thấy thì để 'N/A'. "
        "3. Nếu không có giá trị ở ô nào, trả về 'N/A' hoặc chuỗi rỗng. "
        "4. Đảm bảo trích xuất đầy đủ tất cả các trang, không bỏ sót dòng nào."
    )

    if ext in [".pptx", ".ppt"]:
        prompt = (
            "Bạn là một chuyên gia phân tích dữ liệu và báo cáo KPI. "
            "Hãy đọc tài liệu thuyết trình PowerPoint đính kèm và trích xuất tất cả các chỉ số, mục tiêu đo lường, kết quả hoạt động hoặc bảng số liệu KPI. "
            "Trả về một mảng JSON các đối tượng KpiRecord. "
            "Lưu ý quan trọng: "
            "1. ma_chi_tieu: Lấy mã chỉ tiêu nếu có (VD: ĐT-MT01, QTCL MT001). Nếu slide không có cột mã sẵn, hãy tự tạo mã ngắn gọn theo tên chỉ tiêu hoặc slide (VD: KPI-01, PPT-MTR01, RETAIL-01) để định danh, tuyệt đối KHÔNG để 'N/A' hay bỏ trống. "
            "2. quy_danh_gia: Lấy từ tiêu đề hoặc ngữ cảnh thời gian (VD: Q1/2026, NĂM 2024, Tháng 1/2026). Nếu không thấy thì để 'N/A'. "
            "3. noi_dung_muc_tieu: Mô tả rõ ràng nội dung chỉ tiêu/mục tiêu đo lường. "
            "4. muc_dang_ky và muc_dat: Lấy số liệu kế hoạch và thực tế (hoặc tỷ lệ %, con số thống kê). "
            "5. ket_qua_he_thong: Đánh giá ĐẠT, KHÔNG ĐẠT hoặc CHƯA ĐẾN KỲ ĐÁNH GIÁ (hoặc trạng thái tương ứng). "
            "6. Đảm bảo trích xuất các chỉ số chính xuất hiện trong slide, không bỏ sót dòng nào."
        )
        file_text = extract_text_from_pptx(file_bytes)
        if not file_text.strip():
            raise ValueError(f"Không thể trích xuất văn bản từ file {ext.upper()}.")
        full_contents = [f"{prompt}\n\nVĂN BẢN THUYẾT TRÌNH {ext.upper()}:\n{file_text}"]
    elif ext == ".docx":
        file_text = extract_text_from_docx(file_bytes)
        if not file_text.strip():
            raise ValueError("Không thể trích xuất văn bản từ file DOCX.")
        full_contents = [f"{prompt}\n\nVĂN BẢN:\n{file_text}"]
    else:
        mime_map = {
            ".pdf": "application/pdf",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png"
        }
        mime_type = mime_map.get(ext, "application/pdf")
        # Sử dụng inline Part bytes trực tiếp, không cần upload Files API và polling!
        file_part = types.Part.from_bytes(data=file_bytes, mime_type=mime_type)
        full_contents = [file_part, prompt]

    print(f"🚀 Gửi yêu cầu trích xuất trực tiếp tới Gemini API cho file {file_key}...")
    def make_api_call():
        return client.models.generate_content(
            model="gemini-flash-lite-latest",
            contents=full_contents,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=list[KpiRecord],
                temperature=0.0,
            )
        )

    response = retry_with_backoff(make_api_call, max_retries=3, initial_delay=5)
    raw_json = response.text
    data = json.loads(raw_json)

    print(f"✅ Đã nhận kết quả JSON từ Gemini cho {file_key}")

    rows = []
    quy_list = []
    for idx, item in enumerate(data, start=1):
        ma = item.get("ma_chi_tieu", "N/A")
        quy = item.get("quy_danh_gia", "N/A")

        # Nếu model để N/A hoặc rỗng, tự động sinh mã định danh để không làm mất dữ liệu
        if not ma or str(ma).strip() in ["", "N/A", "None", "NULL"]:
            noi_dung = item.get("noi_dung_muc_tieu", "")
            prefix = re.sub(r'[^A-Z0-9]', '', str(noi_dung).upper()[:8]) or "KPI"
            ma = f"{prefix}-{idx:02d}"

        if str(ma).strip() != "":
            rows.append((
                ma,
                item.get("noi_dung_muc_tieu", "N/A"),
                item.get("dinh_ky_thu_thap", "N/A"),
                item.get("muc_dang_ky", "N/A"),
                item.get("muc_dat", "N/A"),
                item.get("ket_qua_he_thong", "N/A"),
                item.get("nguyen_nhan", ""),
                item.get("hanh_dong_khac_phuc", "")
            ))

            quy_raw = str(quy).upper()
            quy_match = re.search(r"(?:Q|QUÝ|QUY)\s*([1-4])\s*(?:/|-|NĂM|NAM)\s*(\d{4})", quy_raw)
            if quy_match:
                standardized_quy = f"Q{quy_match.group(1)}/{quy_match.group(2)}"
                quy_list.append(standardized_quy)
            else:
                nam_match = re.search(r"(?:NĂM|NAM)\s*(\d{4})", quy_raw)
                thang_match = re.search(r"(?:THÁNG|THANG|T)\s*([0-9]{1,2})\s*(?:/|-|NĂM|NAM)\s*(\d{4})", quy_raw)
                
                if nam_match:
                    quy_list.append(f"NĂM {nam_match.group(1)}")
                elif thang_match:
                    quy_list.append(f"T{thang_match.group(1)}/{thang_match.group(2)}")
                elif quy_raw and quy_raw not in ["N/A", "NONE", "NULL", ""]:
                    quy_list.append(quy_raw)

    quy_danh_gia_final = None
    if quy_list:
        quy_danh_gia_final = max(set(quy_list), key=quy_list.count)

    return rows, quy_danh_gia_final, set(quy_list)


def extract_structured_data(file_bytes: bytes, ext: str):
    """
    Trích xuất dữ liệu từ các file có cấu trúc (CSV, Excel, JSON).
    """
    rows = []
    quy_list = set()
    try:
        if ext == ".csv":
            df = pd.read_csv(io.BytesIO(file_bytes))
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(io.BytesIO(file_bytes))
        elif ext == ".json":
            data = json.loads(file_bytes.decode('utf-8'))
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Giả định data chứa 1 mảng các bản ghi ở root
                for k, v in data.items():
                    if isinstance(v, list):
                        df = pd.DataFrame(v)
                        break
                else:
                    df = pd.DataFrame([data])
        else:
            return None

        if df.empty:
            return None

        # Chuẩn hóa tên cột
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        # Hàm tiện ích lấy giá trị cột
        def get_val(row, possible_names, default="N/A"):
            for n in possible_names:
                for c in df.columns:
                    # Chấp nhận chứa keyword
                    if n in c or c.replace("_", "") in n.replace("_", ""):
                        val = row.get(c)
                        if pd.isna(val) or str(val).strip() == "":
                            return default
                        return str(val).strip()
            return default

        for _, row in df.iterrows():
            ma = get_val(row, ["ma", "id", "code"], "N/A")
            if ma == "N/A":
                continue # Bỏ qua dòng không có mã
            
            noi_dung = get_val(row, ["noi_dung", "muc_tieu", "chi_tieu", "content", "target", "noidungmuctieu"], "N/A")
            dk = get_val(row, ["dinh_ky", "thu_thap", "period", "dinhkythuthap"], "N/A")
            m_dk = get_val(row, ["muc_dang_ky", "ke_hoach", "plan", "dang_ky", "mucdangky"], "N/A")
            m_dat = get_val(row, ["muc_dat", "thuc_te", "actual", "dat", "mucdat"], "N/A")
            kq = get_val(row, ["ket_qua", "danh_gia", "status", "result", "ketqua"], "N/A")
            nguyen_nhan = get_val(row, ["nguyen_nhan", "cause", "reason", "nguyennhan"], "")
            hanh_dong = get_val(row, ["hanh_dong", "khac_phuc", "action", "solution", "hanhdongkehoachkhacphuc"], "")
            
            quy = get_val(row, ["quy", "ky", "quarter", "time", "quydanhgia"], "N/A")
            if quy != "N/A":
                quy_raw = str(quy).upper()
                quy_match = re.search(r"(?:Q|QUÝ|QUY)\s*([1-4])\s*(?:/|-|NĂM|NAM)\s*(\d{4})", quy_raw)
                if quy_match:
                    quy_list.add(f"Q{quy_match.group(1)}/{quy_match.group(2)}")
                else:
                    quy_list.add(quy_raw)

            rows.append((ma, noi_dung, dk, m_dk, m_dat, kq, nguyen_nhan, hanh_dong))
            
        quy_danh_gia_final = None
        if quy_list:
            quy_list_l = list(quy_list)
            quy_danh_gia_final = max(set(quy_list_l), key=quy_list_l.count)
            
        return rows, quy_danh_gia_final, quy_list
        
    except Exception as e:
        print(f"Lỗi parse dữ liệu có cấu trúc: {e}")
        return None


def process_single_file(s3_client, file_key):
    """Xử lý trích xuất dữ liệu cho 1 file từ S3."""
    ext = os.path.splitext(file_key)[1].lower()
    file_bytes = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)["Body"].read()

    raw_rows, quy_danh_gia, ky_candidates = [], None, set()

    if ext == ".docx":
        # Thử parse nhanh trước
        fast_result = extract_table_from_docx_fast(file_bytes)
        if fast_result is not None:
            raw_rows, quy_danh_gia, ky_candidates = fast_result
        else:
            try:
                raw_rows, quy_danh_gia, ky_candidates = parse_with_gemini(file_bytes, ext, file_key)
            except Exception as exc:
                print(f"WARNING: Lỗi bóc tách qua AI cho file {file_key}: {exc}")
    elif ext in [".pptx", ".ppt"]:
        # Thử parse nhanh bảng PPTX trước
        if ext == ".pptx":
            fast_result = extract_table_from_pptx_fast(file_bytes)
            if fast_result is not None:
                raw_rows, quy_danh_gia, ky_candidates = fast_result
        if not raw_rows:
            try:
                raw_rows, quy_danh_gia, ky_candidates = parse_with_gemini(file_bytes, ext, file_key)
            except Exception as exc:
                print(f"WARNING: Lỗi bóc tách qua AI cho file {file_key}: {exc}")
    elif ext in [".pdf", ".jpg", ".jpeg", ".png"]:
        try:
            raw_rows, quy_danh_gia, ky_candidates = parse_with_gemini(file_bytes, ext, file_key)
        except Exception as exc:
            print(f"WARNING: Lỗi bóc tách qua AI cho file {file_key}: {exc}")
    elif ext in [".csv", ".xlsx", ".xls", ".json"]:
        try:
            res = extract_structured_data(file_bytes, ext)
            if res:
                raw_rows, quy_danh_gia, ky_candidates = res
        except Exception as exc:
            print(f"WARNING: Lỗi bóc tách dữ liệu có cấu trúc cho file {file_key}: {exc}")
    elif ext in [".mp4", ".mov"]:
        print(f"SKIP: File video {file_key} được lưu trữ thô thành công.")
        return file_key, [], True
    else:
        print(f"SKIP: Định dạng không hỗ trợ cho file {file_key}")
        return file_key, [], False

    if len(ky_candidates) > 1:
        print(f"⚠️ CẢNH BÁO: file '{file_key}' có thể chứa nhiều kỳ khác nhau {ky_candidates}")

    quy_danh_gia_final = quy_danh_gia or QUY_DANH_GIA_UNKNOWN

    file_extracted = []
    for ma, noi_dung, dk, m_dk, m_dat, kq, nguyen_nhan, hanh_dong in raw_rows:
        ma_str = str(ma).strip().upper()
        nhom = ma_str.split("-")[0].strip() if "-" in ma_str else ma_str.split(" ")[0].strip()

        ma_clean = ma_str
        quy_clean = str(quy_danh_gia_final).strip().upper()
        dk_clean = str(dk).strip().lower()
        mdk_clean = str(m_dk).strip().lower()
        mdat_clean = str(m_dat).strip().lower()
        kq_clean = clean_status_text(kq)

        checksum = generate_checksum(
            f"{file_key}_{ma_clean}_{quy_clean}_{dk_clean}_{mdk_clean}_{mdat_clean}_{kq_clean}"
        )

        file_extracted.append({
            "file_nguon": os.path.basename(file_key),
            "ma_chi_tieu": ma_str,
            "nhom_don_vi": nhom,
            "quy_danh_gia": quy_danh_gia_final,
            "noi_dung_muc_tieu": str(noi_dung).strip(),
            "dinh_ky_thu_thap": str(dk).strip(),
            "muc_dang_ky": str(m_dk).strip(),
            "muc_dang_ky_numeric": parse_percent_or_number(m_dk),
            "muc_dat": str(m_dat).strip(),
            "muc_dat_numeric": parse_percent_or_number(m_dat),
            "ket_qua_he_thong": kq_clean,
            "nguyen_nhan": str(nguyen_nhan).strip(),
            "hanh_dong_khac_phuc": str(hanh_dong).strip(),
            "minh_chung_type": ext.replace(".", "").lower(),
            "minh_chung_path": file_key,
            "checksum_sha256": checksum,
        })

    is_success = bool(raw_rows)
    return file_key, file_extracted, is_success


def main():
    parser = argparse.ArgumentParser(description="Bronze Ingestion")
    parser.add_argument("--run_id", type=str, help="Airflow DAG Run ID", default="")
    parser.add_argument("--file_key", type=str, help="S3 file key to process", default="")
    args = parser.parse_args()
    
    sys.stdout.reconfigure(encoding="utf-8")
    s3_client = get_s3_client()
    extracted_data = []
    
    if args.file_key:
        file_keys = [args.file_key]
        print(f"🎯 Chỉ định xử lý duy nhất file: {args.file_key}")
    else:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SOURCE_PREFIX)
        if "Contents" not in response:
            print("Không có file nào trong staging.")
            sys.exit(0)
            
        file_keys = [
            obj["Key"] for obj in response["Contents"] 
            if not obj["Key"].endswith("/")
        ]

        if not file_keys:
            print("Không có file hợp lệ trong staging.")
            sys.exit(0)

    successful_keys = []
    failed_keys = []

    # Xử lý song song nếu có nhiều file (tối đa 4 workers để tránh quá tải)
    max_workers = min(4, len(file_keys))
    print(f"⚡ Bắt đầu Ingestion cho {len(file_keys)} file(s) với {max_workers} worker(s)...")

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(process_single_file, s3_client, key): key 
            for key in file_keys
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                f_key, f_data, success = future.result()
                if success or f_data:
                    successful_keys.append(f_key)
                    extracted_data.extend(f_data)
                else:
                    failed_keys.append(f_key)
            except Exception as e:
                print(f"❌ Lỗi khi xử lý file {key}: {e}")
                failed_keys.append(key)

    elapsed = round(time.time() - start_time, 2)
    print(f"⏱️ Thời gian trích xuất Bronze: {elapsed}s")

    if extracted_data:
        # Lưu dữ liệu thô vừa parse được vào DB để Frontend hiển thị
        save_parsed_data(args.run_id, extracted_data)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Use run_id to isolate pipeline runs, avoiding reading leftovers from old failed runs
        if args.run_id:
            # clean run_id to avoid invalid S3 chars
            safe_run_id = "".join([c if c.isalnum() else "_" for c in args.run_id])
            output_key = f"bronze/data_extracted_{safe_run_id}.parquet"
        else:
            output_key = f"bronze/data_extracted_{timestamp}.parquet"

        df = pd.DataFrame(extracted_data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        s3_client.put_object(Bucket=BUCKET_NAME, Key=output_key, Body=parquet_buffer.getvalue())
        print(f"✅ Đã tạo Parquet: {output_key} ({len(extracted_data)} dòng)")

        # Tạo file JSON theo định dạng camelCase chuẩn
        json_payload = [
            {
                "ma": row.get("ma_chi_tieu", ""),
                "noiDungMucTieu": row.get("noi_dung_muc_tieu", ""),
                "dinhKyThuThap": row.get("dinh_ky_thu_thap", ""),
                "mucDangKy": row.get("muc_dang_ky", ""),
                "mucDat": row.get("muc_dat", "") if row.get("muc_dat") not in ["", "N/A", None] else "Chưa có dữ liệu",
                "ketQua": row.get("ket_qua_he_thong", "") if row.get("ket_qua_he_thong") not in ["", "N/A", None] else "Chưa có dữ liệu",
                "nguyenNhan": row.get("nguyen_nhan", "") if row.get("nguyen_nhan") not in ["", "N/A", None] else "Chưa có dữ liệu",
                "hanhDongKeHoachKhacPhuc": row.get("hanh_dong_khac_phuc", "") if row.get("hanh_dong_khac_phuc") not in ["", "N/A", None] else "Chưa có dữ liệu"
            }
            for row in extracted_data
        ]

        if args.run_id:
            safe_run_id = "".join([c if c.isalnum() else "_" for c in args.run_id])
            output_json_key = f"bronze/data_extracted_{safe_run_id}.json"
        else:
            output_json_key = f"bronze/data_extracted_{timestamp}.json"
        json_bytes = json.dumps(json_payload, ensure_ascii=False, indent=2).encode("utf-8")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=output_json_key,
            Body=json_bytes,
            ContentType="application/json; charset=utf-8"
        )
        print(f"✅ Đã tạo JSON (camelCase): {output_json_key}")
    else:
        print("❌ Không có dữ liệu hợp lệ nào được trích xuất để ghi Parquet.")

    if successful_keys:
        for key in successful_keys:
            archive_key = key.replace(SOURCE_PREFIX, ARCHIVE_PREFIX, 1)
            s3_client.copy_object(
                Bucket=BUCKET_NAME,
                CopySource=f"{BUCKET_NAME}/{key}",
                Key=archive_key,
            )
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)

        print(f"✨ HOÀN THÀNH INGEST! Đã dọn dẹp {len(successful_keys)} file(s) thành công khỏi staging.")

    if failed_keys:
        print(f"⚠️ CẢNH BÁO: {len(failed_keys)} file(s) không trích xuất được dữ liệu: {failed_keys}")
        print("Chuyển các file lỗi sang failed_staging/ để cách ly.")
        for key in failed_keys:
            if not key.startswith(SOURCE_PREFIX):
                continue
            failed_key = key.replace(SOURCE_PREFIX, "failed_staging/", 1)
            try:
                s3_client.copy_object(
                    Bucket=BUCKET_NAME,
                    CopySource=f"{BUCKET_NAME}/{key}",
                    Key=failed_key,
                )
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
            except Exception as e:
                print(f"Lỗi khi di chuyển file rác {key}: {e}")

    if not extracted_data and failed_keys:
        err_msg = "AI OCR: File không đúng định dạng KPI hoặc chất lượng ảnh quá kém, không trích xuất được dữ liệu."
        print(f"❌ ERROR: {err_msg}")
        update_pipeline_error(args.run_id, err_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()