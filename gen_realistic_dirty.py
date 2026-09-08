import json
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

# Các key bẩn
ma_keys = ["  ID ChiTieu ", "code", "mA_CHi_tieU", "Mã KPI", "mã"]
nd_keys = ["Noi_Dung_Muc_Tieu ", "target", "MUCtieu_noidung", "nội dung"]
quy_keys = ["  quydanhgia ", "quarter", "ky_QUY_Time", "quý"]
kq_keys = ["   KetQua ", "status", "KetQua_Status_Result"]
dk_keys = ["dinh_ky", "chu_ky", "period", "  Dinh Ky Thu Thap  "]
md_keys = ["muc_dat", "thuc_te", "actual", "  Mức Đạt  "]

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

    # Trộn key bẩn nhưng mang giá trị thực tế đẹp
    record[random.choice(ma_keys)] = f"  {ma_kpi}  " # cố tình có khoảng trắng
    record[random.choice(nd_keys)] = f" Mục tiêu chiến lược số {i} của phòng {dept} "
    record[random.choice(quy_keys)] = random.choice([" Q3 năm 2026 ", " quý 3 - 2026 ", "QUY 3 NĂM 2026", "Q3/2026"])
    record[random.choice(kq_keys)] = kq
    record[random.choice(dk_keys)] = random.choices(periods, weights=period_weights)[0]
    record[random.choice(md_keys)] = muc_dat
    
    # Chèn các cột hoàn toàn không liên quan để test độ rác
    if random.random() > 0.5:
        record[f"COT_RAC_{random.randint(1, 10)}"] = "Dữ liệu vô nghĩa " * random.randint(1, 2)
        
    records.append(record)

with open('test_kpi_realistic_dirty.json', 'w', encoding='utf-8') as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print("OK")
