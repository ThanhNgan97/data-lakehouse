import os
import pandas as pd
import requests

# URL của FastAPI Gateway (đảm bảo server FastAPI đang chạy ở cổng 8000)
BASE_URL = "http://localhost:8000/api/v1/lakehouse/ingest"

# Lấy đường dẫn thư mục hiện tại (thư mục api_ingestion_module)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def send_teaching_progress():
    file_path = os.path.join(CURRENT_DIR, "teaching-progress-ctu-ioc.csv")
    print(f"Đang đọc dữ liệu từ: {file_path}")
    df = pd.read_csv(file_path)
    
    for _, row in df.iterrows():
        payload = {
            "ky_danh_gia": "Q2/2026",
            "don_vi_dao_tao": str(row["Đơn vị đào tạo"]).strip(),
            "lop_hp": int(row["Lớp HP"]),
            "dung_tien_do_pct": float(str(row["Đúng tiến độ"]).replace("%", "").replace(",", ".")),
            "hien_dien_pct": float(str(row["Hiện diện"]).replace("%", "").replace(",", ".")),
            "nhap_diem_pct": float(str(row["Nhập điểm"]).replace("%", "").replace(",", ".")),
            "doi_lich": int(row["Đổi lịch"]),
            "diem_phan_hoi_sv": str(row["Phản hồi SV"]),
            "danh_gia": str(row["Đánh giá"])
        }
        
        response = requests.post(f"{BASE_URL}/teaching-progress", json=payload)
        if response.status_code == 201:
            print(f"[THÀNH CÔNG] Đã đẩy tiến độ: {payload['don_vi_dao_tao']}")
        else:
            print(f"[LỖI {response.status_code}] Đơn vị {payload['don_vi_dao_tao']}: {response.text}")

def send_learning_outcomes():
    file_path = os.path.join(CURRENT_DIR, "learning-outcomes-ctu-ioc.csv")
    print(f"\nĐang đọc dữ liệu từ: {file_path}")
    df = pd.read_csv(file_path)
    
    for _, row in df.iterrows():
        payload = {
            "ky_danh_gia": "Q2/2026",
            "chuong_trinh": str(row["Chương trình"]).strip(),
            "sv_theo_hoc": int(str(row["SV theo học"]).replace(".", "").replace(",", "")),
            "qua_hp_pct": float(str(row["Qua HP"]).replace("%", "").replace(",", ".")),
            "gpa_trung_binh": float(str(row["GPA"]).replace(",", ".")),
            "can_bao_hoc_vu": int(row["Cảnh báo"]),
            "nguy_co_nghi_hoc": int(row["Nguy cơ nghỉ"]),
            "dung_tien_do_pct": float(str(row["Đúng tiến độ"]).replace("%", "").replace(",", ".")),
            "xu_huong": str(row["Xu hướng"])
        }
        
        response = requests.post(f"{BASE_URL}/learning-outcomes", json=payload)
        if response.status_code == 201:
            print(f"[THÀNH CÔNG] Đã đẩy kết quả học tập: {payload['chuong_trinh']}")
        else:
            print(f"[LỖI {response.status_code}] Chương trình {payload['chuong_trinh']}: {response.text}")

if __name__ == "__main__":
    try:
        print("--- BẮT ĐẦU GIẢ LẬP ĐẨY DỮ LIỆU TỪ API ---")
        send_teaching_progress()
        send_learning_outcomes()
        print("\n--- HOÀN TẤT TOÀN BỘ QUÁ TRÌNH GIẢ LẬP ---")
    except requests.exceptions.ConnectionError:
        print("\n[LỖI KẾT NỐI] Không thể kết nối tới FastAPI. Hãy chắc chắn server FastAPI đang chạy!")