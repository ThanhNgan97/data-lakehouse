import os
import sys
from minio import Minio
import pandas as pd
import json

import io

def extract_structured_data(file_bytes, ext):
    try:
        if ext in [".xls", ".xlsx"]:
            df = pd.read_excel(io.BytesIO(file_bytes))
        elif ext == ".csv":
            df = pd.read_csv(io.BytesIO(file_bytes))
        elif ext == ".json":
            data = json.loads(file_bytes.decode('utf-8'))
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
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

        def count_header_matches(row_values):
            keywords = ["mã", "ma", "nội dung", "noi dung", "chỉ tiêu", "chi tieu", "kết quả", "ket qua", "định kỳ", "dinh ky", "đạt", "dat"]
            score = 0
            for val in row_values:
                val_str = str(val).lower()
                if any(kw in val_str for kw in keywords):
                    score += 1
            return score

        best_score = count_header_matches(df.columns)
        best_row_idx = -1
        
        for idx in range(min(20, len(df))):
            row_vals = df.iloc[idx].values
            score = count_header_matches(row_vals)
            if score > best_score:
                best_score = score
                best_row_idx = idx
                
        if best_row_idx >= 0 and best_score >= 2:
            new_cols = df.iloc[best_row_idx].values
            df.columns = [str(c) if pd.notna(c) else f"Unnamed_{i}" for i, c in enumerate(new_cols)]
            df = df.iloc[best_row_idx + 1:].reset_index(drop=True)

        df.columns = [str(c).strip().lower() for c in df.columns]
        
        def find_col(possible_names):
            for n in possible_names:
                for c in df.columns:
                    if n in c or c.replace("_", "") in n.replace("_", ""):
                        return c
            return None

        col_ma = find_col(["ma", "id", "code"])
        col_nd = find_col(["noi_dung", "muc_tieu", "chi_tieu", "content", "target", "noidungmuctieu"])
        col_dk = find_col(["dinh_ky", "thu_thap", "period", "dinhkythuthap"])
        col_mdk = find_col(["muc_dang_ky", "ke_hoach", "plan", "dang_ky", "mucdangky"])
        col_mdat = find_col(["muc_dat", "thuc_te", "actual", "dat", "mucdat"])
        col_kq = find_col(["ket_qua", "danh_gia", "status", "result", "ketqua"])
        col_nn = find_col(["nguyen_nhan", "cause", "reason", "nguyennhan"])
        col_hd = find_col(["hanh_dong", "khac_phuc", "action", "solution", "hanhdongkehoachkhacphuc"])
        col_quy = find_col(["quy", "ky", "quarter", "time", "quydanhgia"])

        def get_fast_list(col_name, default):
            if col_name and col_name in df.columns:
                s = df[col_name].fillna(default).astype(str).str.strip()
                s = s.replace("", default)
                return s.tolist()
            else:
                return [default] * len(df)

        list_ma = get_fast_list(col_ma, "N/A")
        list_nd = get_fast_list(col_nd, "N/A")
        list_dk = get_fast_list(col_dk, "Tháng")
        list_mdk = get_fast_list(col_mdk, "100%")
        list_mdat = get_fast_list(col_mdat, "0%")
        list_kq = get_fast_list(col_kq, "Chưa đánh giá")
        list_nn = get_fast_list(col_nn, "")
        list_hd = get_fast_list(col_hd, "")

        quy_danh_gia = None
        if col_quy and col_quy in df.columns:
            q_val = df[col_quy].dropna().astype(str).str.strip().tolist()
            if q_val:
                quy_danh_gia = q_val[0]

        if not col_ma and not col_nd and not col_kq and not col_quy:
            return None

        raw_rows = []
        for ma, nd, dk, m_dk, m_dat, kq, nn, hd in zip(
            list_ma, list_nd, list_dk, list_mdk, list_mdat, list_kq, list_nn, list_hd
        ):
            if ma == "N/A" and nd == "N/A":
                continue
            raw_rows.append((ma, nd, dk, m_dk, m_dat, kq, nn, hd))
            
        return raw_rows, quy_danh_gia, []
    except Exception as e:
        print("Exception in extract:", e)
        return None


minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket = "university-lakehouse"
objects = minio_client.list_objects(bucket, prefix="", recursive=True)
target_file = None
for obj in objects:
    if "bang_tong_hop_kpi_cac_don_vi.xlsx" in obj.object_name:
        target_file = obj.object_name
        break

if not target_file:
    print("Khong tim thay file tren MinIO!")
    sys.exit(1)

print(f"Downloading {target_file}...")
response = minio_client.get_object(bucket, target_file)
file_bytes = response.read()
response.close()
response.release_conn()

print("Testing extract_structured_data...")
try:
    res = extract_structured_data(file_bytes, ".xlsx")
    if res:
        raw_rows, quy_danh_gia, ky_candidates = res
        print("Trích xuất thành công:")
        print(f"Số dòng: {len(raw_rows)}")
        print(f"Quý: {quy_danh_gia}")
        print(f"Ví dụ dòng 1: {raw_rows[0]}")
    else:
        print("Trích xuất thất bại (trả về None).")
except Exception as e:
    import traceback
    traceback.print_exc()
