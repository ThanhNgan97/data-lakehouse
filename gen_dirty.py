import json
import random

records = []
statuses = ["  khong dat  ", " ĐẠT  ", " đạt ", " Không Đạt ", "chưa đánh giá", None]
quarters = [" Q3 năm 2026 ", " quý 3 - 2026 ", "QUY 4 NĂM 2026", "Q1/2026", "Q2-2026  "]
periods = ["Tháng", "Quý", "Năm", "  tháng  ", None]
ma_keys = ["  ID ChiTieu ", "code", "mA_CHi_tieU", "Mã KPI", "mã"]
nd_keys = ["Noi_Dung_Muc_Tieu ", "target", "MUCtieu_noidung", "nội dung"]
quy_keys = ["  quydanhgia ", "quarter", "ky_QUY_Time", "quý"]
kq_keys = ["   KetQua ", "status", "KetQua_Status_Result"]

for i in range(1, 501):
    record = {}
    
    # 5% cơ hội cố tình bỏ quên Mã KPI để test tính năng loại bỏ rác
    if random.random() > 0.05: 
        record[random.choice(ma_keys)] = f"  IT_DEV_{i:04d}  " # Cố tình chèn khoảng trắng
        
    record[random.choice(nd_keys)] = f"   Mục tiêu thử nghiệm siêu bẩn số {i}   "
    record[random.choice(quy_keys)] = random.choice(quarters)
    record[random.choice(kq_keys)] = random.choice(statuses)
    
    # Chèn các cột hoàn toàn không liên quan
    if random.random() > 0.5:
        record[f"COT_RAC_{random.randint(1, 10)}"] = "Nội dung vô giá trị " * random.randint(1, 3)
        
    records.append(record)

with open('test_kpi_dirty_500.json', 'w', encoding='utf-8') as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print("Đã tạo xong file test_kpi_dirty_500.json với 500 dòng!")
