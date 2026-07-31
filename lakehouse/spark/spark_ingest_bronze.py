# -*- coding: utf-8 -*-
"""
spark_ingest_bronze.py (Bản đã cập nhật sử dụng Gemini API cho quá trình Ingestion)
------------------------------------------------------------
TẦNG BRONZE - AI-DRIVEN PARSING
Sử dụng Gemini API để bóc tách thông tin từ PDF/DOCX sang Structured Data.
"""

import io
import os
import sys
import hashlib
import re
import zipfile
import xml.etree.ElementTree as ET
import boto3
import json
import time
import tempfile
from datetime import datetime
import pandas as pd
from pydantic import BaseModel
import argparse
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


def retry_with_backoff(func, max_retries=3, initial_delay=15):
    """
    Retry function with exponential backoff for quota errors.
    """
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_str = str(e)
            # Check for quota/rate limit errors
            if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str or "quota" in error_str.lower():
                if attempt < max_retries - 1:
                    wait_time = initial_delay * (2 ** attempt)
                    print(f"⚠️ Quota exceeded. Retrying in {wait_time}s (Attempt {attempt + 1}/{max_retries})...")
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

# Khai báo cấu trúc Schema ép Gemini trả về
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


def extract_text_from_docx(file_bytes: bytes) -> str:
    """Trích xuất văn bản thô từ file DOCX mà không cần thư viện bên ngoài."""
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
    Sử dụng Gemini File API để phân tích file PDF hoặc văn bản đã trích xuất từ DOCX.
    Trả về (rows, quy_danh_gia, ky_candidates)
    """
    if not client:
        raise ValueError("GEMINI_API_KEY chưa được cấu hình!")

    if ext == ".docx":
        file_text = extract_text_from_docx(file_bytes)
        if not file_text.strip():
            raise ValueError("Không thể trích xuất văn bản từ file DOCX.")

        prompt = (
            "Bạn là một chuyên gia phân tích dữ liệu KPI giáo dục. "
            "Hãy đọc văn bản dưới đây và trích xuất tất cả các dòng chỉ tiêu KPI trong bảng. "
            "Trả về một mảng JSON các đối tượng có các trường (keys) đúng theo cấu trúc được yêu cầu. "
            "Lưu ý: "
            "1. ma_chi_tieu là cột MÃ trong bảng, hãy lấy nguyên văn (VD: ĐT-MT01, QTCL MT001). "
            "2. quy_danh_gia phải có dạng Q[1-4]/[Năm], ví dụ Q1/2026. Nếu không tìm thấy, để trống hoặc 'N/A'. "
            "3. Nếu không có giá trị ở ô nào, trả về 'N/A' hoặc chuỗi rỗng. "
            "4. Đảm bảo trích xuất đầy đủ tất cả các trang, không bỏ sót dòng nào."
            "\n\nVĂN BẢN:\n" + file_text
        )

        def make_api_call():
            return client.models.generate_content(
                model="models/gemini-flash-lite-latest",
                contents=[prompt],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=list[KpiRecord],
                    temperature=0.0,
                )
            )

        response = retry_with_backoff(make_api_call, max_retries=3, initial_delay=15)
    else:
        tmp_path = ""
        uploaded_file = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name

            print(f"Uploading {file_key} to Gemini API...")
            uploaded_file = client.files.upload(file=tmp_path)

            while uploaded_file.state.name == "PROCESSING":
                print(f"File {file_key} is processing, waiting 2 seconds...")
                time.sleep(2)
                uploaded_file = client.files.get(name=uploaded_file.name)

            if uploaded_file.state.name == "FAILED":
                raise ValueError(f"Gemini API failed to process file {file_key}")

            print(f"File {file_key} is ready. Requesting extraction...")
            prompt = (
                "Bạn là một chuyên gia phân tích dữ liệu KPI giáo dục. "
                "Hãy đọc tài liệu đính kèm và trích xuất tất cả các dòng chỉ tiêu KPI trong bảng. "
                "Trả về một mảng JSON các đối tượng có các trường (keys) đúng theo cấu trúc được yêu cầu. "
                "Lưu ý: "
                "1. ma_chi_tieu là cột MÃ trong bảng, hãy lấy nguyên văn (VD: ĐT-MT01, QTCL MT001). "
                "2. quy_danh_gia phải có dạng Q[1-4]/[Năm], ví dụ Q1/2026. Nếu không tìm thấy, để trống hoặc 'N/A'. "
                "3. Nếu không có giá trị ở ô nào, trả về 'N/A' hoặc chuỗi rỗng. "
                "4. Đảm bảo trích xuất đầy đủ tất cả các trang, không bỏ sót dòng nào."
            )

            def make_api_call():
                return client.models.generate_content(
                    model="models/gemini-flash-lite-latest",
                    contents=[uploaded_file, prompt],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=list[KpiRecord],
                        temperature=0.0,
                    )
                )

            response = retry_with_backoff(make_api_call, max_retries=3, initial_delay=15)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

            if uploaded_file:
                try:
                    client.files.delete(name=uploaded_file.name)
                    print(f"Deleted file from Gemini: {uploaded_file.name}")
                except Exception as e:
                    print(f"Failed to delete Gemini file: {e}")

    raw_json = response.text
    data = json.loads(raw_json)

    print("KẾT QUẢ CẤU TRÚC SAU KHI XỬ LÝ GEMINI:")
    try:
        print(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        print(data)

    rows = []
    quy_list = []
    for item in data:
        ma = item.get("ma_chi_tieu", "N/A")
        quy = item.get("quy_danh_gia", "N/A")

        if "MT" in str(ma).upper():
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
            quy_match = re.search(r"(?:Q|QUÝ|QUY)\s*([1-4])\s*/\s*(\d{4})", quy_raw)
            if quy_match:
                standardized_quy = f"Q{quy_match.group(1)}/{quy_match.group(2)}"
                quy_list.append(standardized_quy)

    quy_danh_gia_final = None
    if quy_list:
        quy_danh_gia_final = max(set(quy_list), key=quy_list.count)

    return rows, quy_danh_gia_final, set(quy_list)


def main():
    parser = argparse.ArgumentParser(description="Bronze Ingestion")
    parser.add_argument("--run_id", type=str, help="Airflow DAG Run ID", default="")
    args = parser.parse_args()
    
    sys.stdout.reconfigure(encoding="utf-8")
    s3_client = get_s3_client()
    extracted_data = []
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SOURCE_PREFIX)
    if "Contents" not in response:
        sys.exit(0)

    successful_keys = []
    failed_keys = []
    for obj in response["Contents"]:
        file_key = obj["Key"]

        if file_key.endswith("/"):
            continue
        ext = os.path.splitext(file_key)[1].lower()
        file_bytes = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)["Body"].read()

        raw_rows, quy_danh_gia, ky_candidates = [], None, set()
        
        # Xử lý bằng Gemini thay vì pdfplumber/docx
        if ext in [".pdf", ".docx", ".jpg", ".jpeg", ".png"]:
            try:
                raw_rows, quy_danh_gia, ky_candidates = parse_with_gemini(file_bytes, ext, file_key)
                
                # Rate limiting: wait 65 seconds between API calls to respect per-minute quota
                print("Rate limiting: waiting 30 seconds before next API call...")
                time.sleep(30)  # Wait 30 seconds to avoid hitting the quota limi
            except Exception as exc:
                print(f"WARNING: Lỗi bóc tách qua AI cho file {file_key}: {exc}")
        elif ext in [".mp4", ".mov"]:
            print(f"SKIP: File video {file_key} được lưu trữ thô thành công nhưng chưa trích xuất (chờ Phase 2).")
            successful_keys.append(file_key) # Đánh dấu thành công để archive
        else:
            print(f"SKIP: Định dạng không hỗ trợ cho file {file_key}")

        if len(ky_candidates) > 1:
            print(
                f"⚠️  CẢNH BÁO: file '{file_key}' có vẻ chứa nhiều kỳ khác nhau {ky_candidates}, "
                f"chỉ đang gán '{quy_danh_gia}' cho toàn bộ dữ liệu trong file này. "
                f"Cần kiểm tra thủ công."
            )

        quy_danh_gia_final = quy_danh_gia or QUY_DANH_GIA_UNKNOWN
        if quy_danh_gia_final == QUY_DANH_GIA_UNKNOWN and raw_rows:
            print(
                f"⚠️  CẢNH BÁO: không xác định được kỳ đánh giá trong file '{file_key}'. "
                f"Dữ liệu vẫn được ingest vào Bronze với quy_danh_gia='{QUY_DANH_GIA_UNKNOWN}' "
                f"để admin kiểm tra thủ công ở bước Silver (KHÔNG chặn upload)."
            )

        for ma, noi_dung, dk, m_dk, m_dat, kq, nguyen_nhan, hanh_dong in raw_rows:
            ma_str = str(ma).strip().upper()
            if "-" in ma_str:
                nhom = ma_str.split("-")[0].strip()
            else:
                nhom = ma_str.split(" ")[0].strip()
            
          
            ma_clean = ma_str
            quy_clean = str(quy_danh_gia_final).strip().upper()
            dk_clean = str(dk).strip().lower()
            mdk_clean = str(m_dk).strip().lower()
            mdat_clean = str(m_dat).strip().lower()
            kq_clean = clean_status_text(kq)
            
            checksum = generate_checksum(
                f"{file_key}_{ma_clean}_{quy_clean}_{dk_clean}_{mdk_clean}_{mdat_clean}_{kq_clean}"
            )
            
            extracted_data.append({
                "file_nguon": os.path.basename(file_key),
                "ma_chi_tieu": str(ma).strip().upper(),
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

        if raw_rows:
            successful_keys.append(file_key)
        else:
            failed_keys.append(file_key)

    if extracted_data:
        # Lưu dữ liệu thô vừa parse được vào DB để Frontend hiển thị cho người dùng xem
        save_parsed_data(args.run_id, extracted_data)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_key = f"bronze/data_extracted_{timestamp}.parquet"

        df = pd.DataFrame(extracted_data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        s3_client.put_object(Bucket=BUCKET_NAME, Key=output_key, Body=parquet_buffer.getvalue())
        print(f"✅ Đã tạo Parquet: {output_key} ({len(extracted_data)} dòng)")
    else:
        print("❌ Không có dữ liệu hợp lệ nào được trích xuất để ghi Parquet.")

    if successful_keys:
        for key in successful_keys:
            archive_key = key.replace(SOURCE_PREFIX, ARCHIVE_PREFIX, 1)
            s3_client.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={"Bucket": BUCKET_NAME, "Key": key},
                Key=archive_key,
            )
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)

        print(f"✨ HOÀN THÀNH INGEST! Đã dọn dẹp {len(successful_keys)} file(s) thành công khỏi staging.")

    if failed_keys:
        print(f"⚠️ CẢNH BÁO: {len(failed_keys)} file(s) trong staging không trích xuất được dữ liệu: {failed_keys}")
        print("Các file lỗi được giữ nguyên ở staging/ để kiểm tra và xử lý.")

    if not extracted_data and failed_keys:
        err_msg = "AI OCR: File không đúng định dạng KPI hoặc chất lượng ảnh quá kém, không trích xuất được dữ liệu."
        print(f"❌ ERROR: {err_msg}")
        update_pipeline_error(args.run_id, err_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()