import pandas as pd
import random

records = []

# Dữ liệu thực tế để biểu đồ đẹp
departments = ["HT", "PM", "QTCL", "RD", "VP", "ĐT"]
periods = ["Tháng", "Quý", "Năm"]
period_weights = [0.5, 0.3, 0.2]

# Trạng thái bẩn nhưng map về đúng 3 trạng thái chuẩn
status_dat = [" ĐẠT ", "đạt", "  Dat  ", " Đạt"]
status_khong_dat = ["Không Đạt", " khong dat ", "KHÔNG ĐẠT", "  Không đạt  "]
status_chua = ["chưa đánh giá", " Chưa đến kỳ đánh giá ", "chua danh gia", "CHƯA ĐÁNH GIÁ"]

for i in range(1, 501):
    record = {}
    
    # Chọn phòng ban và tạo mã thực tế (VD: HT-MT001)
    dept = random.choice(departments)
    ma_kpi = f"{dept}-MT{i:03d}"
    
    # Random trạng thái theo tỷ lệ (60% Đạt, 25% Không đạt, 15% Chưa đánh giá)
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

    # Trộn key bẩn bằng cách định nghĩa các tên cột "trời ơi đất hỡi" cho Excel
    record["  ID ChiTieu "] = f"  {ma_kpi}  " # cố tình có khoảng trắng
    record["Noi_Dung_Muc_Tieu "] = f" Mục tiêu chiến lược Excel số {i} của phòng {dept} "
    record["  quydanhgia "] = random.choice([" Q3 năm 2026 ", " quý 3 - 2026 ", "QUY 3 NĂM 2026", "Q3/2026"])
    record["   KetQua "] = kq
    record["  Dinh Ky Thu Thap  "] = random.choices(periods, weights=period_weights)[0]
    record["  Mức Đạt  "] = muc_dat
    
    # Cột rác
    if random.random() > 0.5:
        record["Ghi chú lôm côm"] = "Cột này chả có tác dụng gì"
        
    records.append(record)

df = pd.DataFrame(records)
# Lưu thành file Excel
df.to_excel('test_kpi_realistic_dirty.xlsx', index=False)
print("Tạo thành công file test_kpi_realistic_dirty.xlsx!")
