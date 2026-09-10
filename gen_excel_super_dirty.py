import pandas as pd
import random

departments = ["HT", "PM", "QTCL", "RD", "VP", "ĐT"]
periods = ["Tháng", "Quý", "Năm"]
period_weights = [0.5, 0.3, 0.2]
status_dat = [" ĐẠT ", "đạt", "Đạt"]
status_khong_dat = ["Không Đạt", "KHÔNG ĐẠT"]
status_chua = ["Chưa đánh giá", "CHƯA ĐÁNH GIÁ"]

# Tạo list chứa các dòng (để có thể giả lập Tựa đề và các dòng trống ở trên)
rows = []

# --- PHẦN JUNK (RÁC / TỰA ĐỀ) Ở TRÊN CÙNG ---
rows.append(["BÁO CÁO TỔNG HỢP KPI TOÀN TRƯỜNG NĂM 2026", "", "", "", "", ""])
rows.append(["Ngày xuất báo cáo: 08/09/2026", "", "", "", "", ""])
rows.append(["", "", "", "", "", ""]) # Dòng trống
rows.append(["Người xuất: Nguyễn Văn A", "", "", "", "", ""])
rows.append(["", "", "", "", "", ""]) # Dòng trống

# --- DÒNG TIÊU ĐỀ THỰC SỰ (Sử dụng chuẩn tiếng Việt có dấu) ---
headers = ["  Mã Chỉ Tiêu  ", "Nội dung", " Chu kỳ ", "  Kế hoạch ", " Thực Tế", "Trạng thái"]
rows.append(headers)

# --- PHẦN DỮ LIỆU ---
for i in range(1, 501):
    dept = random.choice(departments)
    ma_kpi = f"{dept}-MT{i:03d}"
    
    rand_st = random.random()
    if rand_st < 0.6:
        kq = random.choice(status_dat)
        muc_dat = f"{random.randint(90, 100)}%"
    elif rand_st < 0.85:
        kq = random.choice(status_khong_dat)
        muc_dat = f"{random.randint(40, 85)}%"
    else:
        kq = random.choice(status_chua)
        muc_dat = "0%"

    nd = f"Mục tiêu thử thách Excel tiếng Việt số {i} ({dept})"
    chu_ky = random.choices(periods, weights=period_weights)[0]
    
    row_data = [
        ma_kpi,
        nd,
        chu_ky,
        "100%", # Kế hoạch
        muc_dat, # Thực tế
        kq # Trạng thái
    ]
    rows.append(row_data)

# DataFrame từ mảng 2 chiều (Header mặc định là dòng số 0)
df = pd.DataFrame(rows)

# Lưu thành file Excel mà không có header thực của Pandas (header mặc định là 0, 1, 2, 3...)
df.to_excel('Bao_cao_tien_do_KPI_Truong_2026.xlsx', index=False, header=False)
print("Tao thanh cong file Bao_cao_tien_do_KPI_Truong_2026.xlsx!")
