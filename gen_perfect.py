import json
import random

records = []
departments = ["HT", "PM", "QTCL", "RD", "VP", "ĐT"]
periods = ["Tháng", "Quý", "Năm"]
period_weights = [0.5, 0.3, 0.2]

for i in range(1, 501):
    dept = random.choice(departments)
    ma_kpi = f"{dept}-MT{i:03d}"
    
    rand_st = random.random()
    if rand_st < 0.6:
        kq = "ĐẠT"
        muc_dat = f"{random.randint(90, 100)}%"
        nn = ""
        hd = ""
    elif rand_st < 0.85:
        kq = "KHÔNG ĐẠT"
        muc_dat = f"{random.randint(40, 85)}%"
        nn = "Chưa hoàn thành do thiếu nhân lực"
        hd = "Bổ sung nguồn lực"
    else:
        kq = "CHƯA ĐẾN KỲ ĐÁNH GIÁ"
        muc_dat = "0%"
        nn = ""
        hd = ""

    # Cấu trúc JSON CHUẨN 100% khớp tuyệt đối với Lakehouse
    record = {
        "ma_chi_tieu": ma_kpi,
        "noi_dung_muc_tieu": f"Mục tiêu chiến lược chuẩn số {i} của phòng {dept}",
        "quy_danh_gia": "Q3/2026",
        "dinh_ky_thu_thap": random.choices(periods, weights=period_weights)[0],
        "muc_dang_ky": "100%",
        "muc_dat": muc_dat,
        "ket_qua_he_thong": kq,
        "nguyen_nhan": nn,
        "hanh_dong_khac_phuc": hd
    }
        
    records.append(record)

with open('test_kpi_perfect.json', 'w', encoding='utf-8') as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
