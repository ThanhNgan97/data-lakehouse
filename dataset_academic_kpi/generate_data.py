#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script sinh dữ liệu mẫu học phần & KPI đào tạo cho PostgreSQL
Khớp 100% các số liệu báo cáo giám sát đào tạo theo đơn vị.
"""

import os
import csv
import random
from datetime import datetime, timedelta

# Định nghĩa 10 đơn vị đào tạo theo đúng thứ tự và số liệu chỉ tiêu trong báo cáo
UNITS_CONFIG = [
    {
        'stt': 1,
        'id': 'BK',
        'name': 'Trường Bách khoa',
        'type': 'Trường',
        'classes': 684,
        'target_tien_do': 92.8,
        'target_hien_dien': 91.6,
        'target_nhap_diem': 88.4,
        'target_doi_lich': 18,
        'target_feedback': 4.21,
        'eval': 'Cần cải thiện',
        'course_prefixes': ['ME', 'EE', 'CE', 'AE'],
        'sample_courses': [
            'Cơ học kỹ thuật', 'Vật liệu cơ khí', 'Thiết kế chi tiết máy',
            'Kỹ thuật nhiệt', 'Kỹ thuật điện đại cương', 'Tự động hóa sản xuất',
            'Sức bền vật liệu', 'Kỹ thuật điều khiển', 'Vẽ kỹ thuật công nghiệp'
        ]
    },
    {
        'stt': 2,
        'id': 'KHTN',
        'name': 'Trường Khoa học Tự nhiên',
        'type': 'Trường',
        'classes': 426,
        'target_tien_do': 93.7,
        'target_hien_dien': 92.4,
        'target_nhap_diem': 90.1,
        'target_doi_lich': 11,
        'target_feedback': 4.28,
        'eval': 'Cần theo dõi',
        'course_prefixes': ['MATH', 'PHYS', 'CHEM', 'BIO'],
        'sample_courses': [
            'Giải tích 1', 'Giải tích 2', 'Đại số tuyến tính',
            'Vật lý đại cương 1', 'Vật lý lượng tử', 'Hóa vô cơ căn bản',
            'Hóa phân tích', 'Xác suất và thống kê', 'Hóa học hữu cơ'
        ]
    },
    {
        'stt': 3,
        'id': 'KT',
        'name': 'Trường Kinh tế',
        'type': 'Trường',
        'classes': 712,
        'target_tien_do': 95.2,
        'target_hien_dien': 93.8,
        'target_nhap_diem': 91.7,
        'target_doi_lich': 9,
        'target_feedback': 4.32,
        'eval': 'Cần theo dõi',
        'course_prefixes': ['ECON', 'FIN', 'MKT', 'ACC'],
        'sample_courses': [
            'Kinh tế vi mô 1', 'Kinh tế vĩ mô 1', 'Nguyên lý kế toán',
            'Quản trị học', 'Tài chính tiền tệ', 'Marketing căn bản',
            'Quản trị nguồn nhân lực', 'Kinh tế quốc tế', 'Phân tích tài chính'
        ]
    },
    {
        'stt': 4,
        'id': 'CNTT',
        'name': 'Trường CNTT&TT',
        'type': 'Trường',
        'classes': 498,
        'target_tien_do': 96.1,
        'target_hien_dien': 94.2,
        'target_nhap_diem': 92.6,
        'target_doi_lich': 8,
        'target_feedback': 4.38,
        'eval': 'Đạt',
        'course_prefixes': ['CT', 'CS', 'SE', 'NET'],
        'sample_courses': [
            'Cơ sở dữ liệu', 'Lập trình hướng đối tượng', 'Cấu trúc dữ liệu và giải thuật',
            'Mạng máy tính', 'Kiến trúc máy tính', 'Hệ điều hành',
            'Phát triển ứng dụng Web', 'Trí tuệ nhân tạo', 'Học máy nâng cao'
        ]
    },
    {
        'stt': 5,
        'id': 'NN',
        'name': 'Trường Nông nghiệp',
        'type': 'Trường',
        'classes': 516,
        'target_tien_do': 96.8,
        'target_hien_dien': 94.9,
        'target_nhap_diem': 94.1,
        'target_doi_lich': 6,
        'target_feedback': 4.41,
        'eval': 'Đạt',
        'course_prefixes': ['AGRI', 'HORT', 'SOIL', 'CROP'],
        'sample_courses': [
            'Nông học đại cương', 'Khoa học đất trồng', 'Bảo vệ thực vật',
            'Sinh lý thực vật nông nghiệp', 'Công nghệ canh tác thông minh',
            'Dinh dưỡng cây trồng', 'Di truyền chọn giống cây trồng', 'Côn trùng nông nghiệp'
        ]
    },
    {
        'stt': 6,
        'id': 'SP',
        'name': 'Trường Sư phạm',
        'type': 'Trường',
        'classes': 438,
        'target_tien_do': 97.4,
        'target_hien_dien': 95.6,
        'target_nhap_diem': 95.2,
        'target_doi_lich': 5,
        'target_feedback': 4.47,
        'eval': 'Tốt',
        'course_prefixes': ['EDU', 'PSY', 'PED', 'MET'],
        'sample_courses': [
            'Tâm lý học lứa tuổi', 'Giáo dục học đại cương', 'Lý luận dạy học hiện đại',
            'Ứng dụng CNTT trong dạy học', 'Phương pháp giảng dạy chuyên ngành',
            'Đo lường và đánh giá giáo dục', 'Kỹ năng giao tiếp sư phạm'
        ]
    },
    {
        'stt': 7,
        'id': 'TS',
        'name': 'Trường Thủy sản',
        'type': 'Trường',
        'classes': 306,
        'target_tien_do': 97.8,
        'target_hien_dien': 95.9,
        'target_nhap_diem': 95.8,
        'target_doi_lich': 4,
        'target_feedback': 4.52,
        'eval': 'Tốt',
        'course_prefixes': ['AQUA', 'FISH', 'MARI'],
        'sample_courses': [
            'Kỹ thuật nuôi giáp xác', 'Ngư loại học chuyên đề', 'Bệnh học thủy sản đại cương',
            'Quản lý chất lượng nước ao nuôi', 'Dinh dưỡng thức ăn thủy sản',
            'Chế biến sản phẩm thủy sản', 'Khai thác thủy sản bền vững'
        ]
    },
    {
        'stt': 8,
        'id': 'NNGU',
        'name': 'Khoa Ngoại ngữ',
        'type': 'Khoa',
        'classes': 348,
        'target_tien_do': 98.1,
        'target_hien_dien': 96.2,
        'target_nhap_diem': 96.4,
        'target_doi_lich': 3,
        'target_feedback': 4.55,
        'eval': 'Tốt',
        'course_prefixes': ['ENG', 'FRA', 'JPN'],
        'sample_courses': [
            'Tiếng Anh học thuật 1', 'Ngữ pháp thực hành nâng cao', 'Kỹ năng Viết học thuật',
            'Kỹ năng Đọc hiểu mở rộng', 'Tiếng Anh thương mại căn bản',
            'Biên phiên dịch cơ sở', 'Phát âm tiếng Anh chuẩn'
        ]
    },
    {
        'stt': 9,
        'id': 'KHXHNV',
        'name': 'Trường KHXH&NV',
        'type': 'Trường',
        'classes': 274,
        'target_tien_do': 98.4,
        'target_hien_dien': 96.8,
        'target_nhap_diem': 97.1,
        'target_doi_lich': 2,
        'target_feedback': 4.58,
        'eval': 'Tốt',
        'course_prefixes': ['SOC', 'HIS', 'LIT', 'POL'],
        'sample_courses': [
            'Triết học Mác - Lênin', 'Lịch sử văn minh thế giới', 'Cơ sở văn hóa Việt Nam',
            'Nhập môn Xã hội học', 'Kỹ thuật văn bản và lưu trữ',
            'Truyền thông đại chúng', 'Văn học Việt Nam hiện đại'
        ]
    },
    {
        'stt': 10,
        'id': 'CNSHTP',
        'name': 'Viện CNSH&TP',
        'type': 'Viện',
        'classes': 184,
        'target_tien_do': 98.9,
        'target_hien_dien': 97.1,
        'target_nhap_diem': 97.6,
        'target_doi_lich': 1,
        'target_feedback': 4.61,
        'eval': 'Xuất sắc',
        'course_prefixes': ['BIOT', 'FOOD', 'CHEMB'],
        'sample_courses': [
            'Công nghệ sinh học vi sinh', 'Hóa sinh thực phẩm ứng dụng', 'Vi sinh vật học thực phẩm',
            'Công nghệ chế biến rau quả', 'Đảm bảo chất lượng và ATTP',
            'Kỹ thuật phân tích thực phẩm', 'Công nghệ lên men hiện đại'
        ]
    }
]

HO_LOT = ['Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Huỳnh', 'Phan', 'Vũ', 'Võ', 'Đặng', 'Bùi', 'Đỗ', 'Hồ', 'Ngô', 'Dương', 'Lý']
TEN_DEM = ['Văn', 'Thị', 'Đức', 'Hải', 'Thanh', 'Hữu', 'Minh', 'Ngọc', 'Quốc', 'Xuân', 'Kim', 'Trọng', 'Bảo', 'Anh']
TEN = ['An', 'Bình', 'Cường', 'Dũng', 'Giang', 'Hà', 'Hải', 'Hiếu', 'Hoàng', 'Hùng', 'Huy', 'Khoa', 'Linh', 'Long', 'Mai', 'Nam', 'Nghĩa', 'Phong', 'Phúc', 'Quân', 'Sơn', 'Tâm', 'Thắng', 'Thảo', 'Trang', 'Trung', 'Tuấn', 'Tùng', 'Vinh', 'Vũ']

LY_DO_DOI_LICH = [
    'Giảng viên công tác chuyên môn đột xuất',
    'Trùng lịch bảo vệ luận án tiến sĩ',
    'Tham gia đoàn đánh giá kiểm định chất lượng',
    'Báo ốm đột xuất, đổi sang buổi học bù',
    'Phòng học bảo trì hệ thống máy chiếu/điều hòa',
    'Trùng lịch hội nghị khoa học quốc tế',
    'Học phần chuyển sang phòng máy vi tính chuyên dụng'
]

def generate_teacher_name(rng):
    return f"{rng.choice(HO_LOT)} {rng.choice(TEN_DEM)} {rng.choice(TEN)}"

def generate_bounded_values(n, target_avg, min_val, max_val, decimals=2, rng=None):
    """
    Sinh dãy n giá trị ngẫu nhiên trong khoảng [min_val, max_val]
    sao cho giá trị trung bình chính xác tới số chữ số thập phân (decimals) khớp target_avg.
    """
    if rng is None:
        rng = random.Random(42)
    values = [target_avg] * n
    # Thêm độ lệch ngẫu nhiên
    for _ in range(n * 6):
        i = rng.randint(0, n - 1)
        j = rng.randint(0, n - 1)
        if i == j:
            continue
        delta = round(rng.uniform(0.05, 1.6), decimals)
        if values[i] + delta <= max_val and values[j] - delta >= min_val:
            values[i] = round(values[i] + delta, decimals)
            values[j] = round(values[j] - delta, decimals)
            
    # Bù trừ sai số tổng để khớp chính xác target_avg
    target_sum = round(target_avg * n, decimals)
    curr_sum = round(sum(values), decimals)
    diff = round(target_sum - curr_sum, decimals)
    step = 10**(-decimals)
    
    max_loops = 100000
    loops = 0
    while abs(diff) >= step / 2 and loops < max_loops:
        loops += 1
        idx = rng.randint(0, n - 1)
        if diff > 0 and values[idx] + step <= max_val:
            values[idx] = round(values[idx] + step, decimals)
            diff = round(diff - step, decimals)
        elif diff < 0 and values[idx] - step >= min_val:
            values[idx] = round(values[idx] - step, decimals)
            diff = round(diff + step, decimals)
            
    return values

def main():
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    os.makedirs(output_dir, exist_ok=True)
    
    rng = random.Random(20261008)
    
    # 1. dm_don_vi_dao_tao
    rows_don_vi = []
    for u in UNITS_CONFIG:
        rows_don_vi.append({
            'ma_don_vi': u['id'],
            'ten_don_vi': u['name'],
            'loai_don_vi': u['type'],
            'thu_tu_hien_thi': u['stt']
        })
        
    # Chuẩn bị danh sách các bảng con
    rows_lop_hp = []
    rows_tien_do = []
    rows_diem_danh = []
    rows_nhap_diem = []
    rows_doi_lich = []
    rows_khao_sat = []
    
    doi_lich_counter = 1
    khao_sat_counter = 1
    
    base_date = datetime(2025, 9, 15)
    
    for u in UNITS_CONFIG:
        n = u['classes']
        td_vals = generate_bounded_values(n, u['target_tien_do'], 78.0, 100.0, 1, rng)
        hd_vals = generate_bounded_values(n, u['target_hien_dien'], 80.0, 100.0, 1, rng)
        nd_vals = generate_bounded_values(n, u['target_nhap_diem'], 70.0, 100.0, 1, rng)
        fb_vals = generate_bounded_values(n, u['target_feedback'], 3.20, 5.00, 2, rng)
        
        class_ids = []
        
        for i in range(n):
            lhp_id = f"{u['id']}_HP{i+1:04d}"
            class_ids.append(lhp_id)
            
            mon_hoc = u['sample_courses'][i % len(u['sample_courses'])]
            ma_mon = f"{u['course_prefixes'][i % len(u['course_prefixes'])]}{100 + (i % 899)}"
            so_tc = rng.choice([2, 3, 3, 4])
            so_sv = rng.randint(35, 80)
            gv_name = generate_teacher_name(rng)
            
            # Bảng lop_hoc_phan
            rows_lop_hp.append({
                'ma_lop_hp': lhp_id,
                'ma_don_vi': u['id'],
                'ma_mon_hoc': ma_mon,
                'ten_mon_hoc': mon_hoc,
                'so_tin_chi': so_tc,
                'so_sinh_vien': so_sv,
                'giang_vien': gv_name,
                'hoc_ky': 'HK1',
                'nam_hoc': '2025-2026'
            })
            
            # Bảng tien_do_giang_day
            so_tiet_kh = so_tc * 15
            so_tiet_thuc = round(so_tiet_kh * (td_vals[i] / 100.0))
            trang_thai_td = 'Đúng hạn' if td_vals[i] >= 95.0 else 'Cần đẩy nhanh'
            rows_tien_do.append({
                'ma_lop_hp': lhp_id,
                'so_tiet_ke_hoach': so_tiet_kh,
                'so_tiet_thuc_day': so_tiet_thuc,
                'ty_le_dung_tien_do': f"{td_vals[i]:.1f}",
                'trang_thai_tien_do': trang_thai_td
            })
            
            # Bảng diem_danh_lop_hp
            tong_luot_dd = so_sv * 10
            luot_co_mat = round(tong_luot_dd * (hd_vals[i] / 100.0))
            rows_diem_danh.append({
                'ma_lop_hp': lhp_id,
                'tong_luot_diem_danh': tong_luot_dd,
                'so_luot_co_mat': luot_co_mat,
                'ty_le_hien_dien': f"{hd_vals[i]:.1f}"
            })
            
            # Bảng tien_do_nhap_diem
            so_cot_qd = 3
            so_cot_nhap = 3 if nd_vals[i] >= 95.0 else (2 if nd_vals[i] >= 66.0 else 1)
            han_chot = (base_date + timedelta(days=90)).strftime('%Y-%m-%d')
            ngay_ht = (base_date + timedelta(days=85 + rng.randint(-5, 10))).strftime('%Y-%m-%d')
            trang_thai_nd = 'Đã hoàn thành' if nd_vals[i] >= 95.0 else 'Đang cập nhật'
            rows_nhap_diem.append({
                'ma_lop_hp': lhp_id,
                'so_cot_diem_quy_dinh': so_cot_qd,
                'so_cot_diem_da_nhap': so_cot_nhap,
                'ty_le_nhap_diem': f"{nd_vals[i]:.1f}",
                'han_chot_nhap_diem': han_chot,
                'ngay_hoan_thanh': ngay_ht,
                'trang_thai_nhap_diem': trang_thai_nd
            })
            
            # Bảng khao_sat_sinh_vien
            so_sv_ks = max(20, round(so_sv * rng.uniform(0.75, 0.95)))
            mc_hai_long = 'Rất hài lòng' if fb_vals[i] >= 4.5 else ('Hài lòng' if fb_vals[i] >= 4.0 else 'Bình thường')
            rows_khao_sat.append({
                'ma_khao_sat': f"KS_{khao_sat_counter:05d}",
                'ma_lop_hp': lhp_id,
                'so_sv_khao_sat': so_sv_ks,
                'diem_danh_gia': f"{fb_vals[i]:.2f}",
                'muc_do_hai_long': mc_hai_long
            })
            khao_sat_counter += 1
            
        # Bảng doi_lich_giang_day: Chọn đúng số lượng lớp tương ứng target_doi_lich
        sample_classes_for_reschedule = rng.sample(class_ids, u['target_doi_lich'])
        for cl in sample_classes_for_reschedule:
            day_offset = rng.randint(10, 70)
            ngay_yeu_cau = (base_date + timedelta(days=day_offset)).strftime('%Y-%m-%d')
            gio_hoc = rng.choice(['07:00:00', '09:30:00', '13:30:00', '15:30:00'])
            tg_cu = f"{(base_date + timedelta(days=day_offset+2)).strftime('%Y-%m-%d')} {gio_hoc}"
            tg_moi = f"{(base_date + timedelta(days=day_offset+5)).strftime('%Y-%m-%d')} {gio_hoc}"
            ly_do = rng.choice(LY_DO_DOI_LICH)
            rows_doi_lich.append({
                'ma_yeu_cau': f"DL_{doi_lich_counter:04d}",
                'ma_lop_hp': cl,
                'ngay_yeu_cau': ngay_yeu_cau,
                'thoi_gian_cu': tg_cu,
                'thoi_gian_moi': tg_moi,
                'ly_do': ly_do,
                'trang_thai': 'Đã duyệt'
            })
            doi_lich_counter += 1

    # Xuất ra các file CSV
    files_to_export = [
        ('dm_don_vi_dao_tao.csv', rows_don_vi),
        ('lop_hoc_phan.csv', rows_lop_hp),
        ('tien_do_giang_day.csv', rows_tien_do),
        ('diem_danh_lop_hp.csv', rows_diem_danh),
        ('tien_do_nhap_diem.csv', rows_nhap_diem),
        ('doi_lich_giang_day.csv', rows_doi_lich),
        ('khao_sat_sinh_vien.csv', rows_khao_sat)
    ]
    
    print(f"Bắt đầu xuất CSV vào thư mục: {output_dir}")
    for filename, rows in files_to_export:
        filepath = os.path.join(output_dir, filename)
        if not rows:
            continue
        fieldnames = list(rows[0].keys())
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f" -> Đã tạo: {filename:<25} ({len(rows):>5} dòng)")
        
    print("\nHoàn tất tạo 100% dữ liệu mẫu.")

if __name__ == '__main__':
    main()
