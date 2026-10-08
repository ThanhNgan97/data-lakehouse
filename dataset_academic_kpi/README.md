# BỘ DỮ LIỆU MẪU & SCHEMA POSTGRESQL: GIÁM SÁT TIẾN ĐỘ & CHẤT LƯỢNG ĐÀO TẠO (IOC)

Tập hợp dữ liệu mẫu và mã nguồn SQL phục vụ bài toán **Reverse Engineering** từ Dashboard điều hành IOC cấp đơn vị đào tạo.

---

## 1. Cấu trúc thư mục

```text
dataset_academic_kpi/
├── data/                       # Chứa 7 file CSV dữ liệu thành phần (UTF-8)
│   ├── dm_don_vi_dao_tao.csv   # 10 bản ghi (Trường, Khoa, Viện)
│   ├── lop_hoc_phan.csv        # 4,386 lớp học phần
│   ├── tien_do_giang_day.csv   # 4,386 bản ghi theo dõi tiến độ
│   ├── diem_danh_lop_hp.csv    # 4,386 bản ghi tỷ lệ hiện diện/chuyên cần
│   ├── tien_do_nhap_diem.csv   # 4,386 bản ghi tiến độ nộp điểm
│   ├── doi_lich_giang_day.csv  # 67 yêu cầu đổi/bù lịch giảng dạy
│   └── khao_sat_sinh_vien.csv  # 4,386 bản ghi đánh giá phản hồi sinh viên
├── sql/                        # Kịch bản SQL cho PostgreSQL
│   ├── 01_create_schema.sql    # Khởi tạo bảng, khóa chính, khóa ngoại, chỉ mục
│   ├── 02_import_csv.sql       # Nạp dữ liệu CSV vào database
│   └── 03_create_view_kpi.sql  # View tổng hợp khớp 100% số liệu báo cáo
├── generate_data.py            # Script Python sinh dữ liệu ràng buộc toán học
└── README.md                   # Tài liệu hướng dẫn sử dụng
```

---

## 2. Mô hình quan hệ thực thể (ERD)

```text
  [dm_don_vi_dao_tao] (1)
          │
          └──< (N) [lop_hoc_phan] (1) ──┬── (1) [tien_do_giang_day]
                                        ├── (1) [diem_danh_lop_hp]
                                        ├── (1) [tien_do_nhap_diem]
                                        ├── (1) [khao_sat_sinh_vien]
                                        └── (N) [doi_lich_giang_day]
```

---

## 3. Hướng dẫn chạy trên PostgreSQL Local

### Chạy qua Docker Container hiện có (`demo-postgres`)
<span style="color: red; font-size: 50px; font-weight: bold;">
Tạo Database có tên là `ctu_ioc`
</span>

```
# Copy thư mục vào container
docker cp dataset_academic_kpi demo-postgres:/tmp/

# Khởi tạo bảng
docker exec -i demo-postgres psql -U postgres -d university_db -f /tmp/dataset_academic_kpi/sql/01_create_schema.sql

# Nạp dữ liệu CSV
docker exec -i demo-postgres psql -U postgres -d university_db -f /tmp/dataset_academic_kpi/sql/02_import_csv.sql

# Tạo View & hiển thị kết quả
docker exec -i demo-postgres psql -U postgres -d university_db -f /tmp/dataset_academic_kpi/sql/03_create_view_kpi.sql
```

---

## 4. Cách sinh lại dữ liệu (Tùy biến)

Nếu muốn sinh lại dữ liệu với hạt giống (seed) ngẫu nhiên khác hoặc điều chỉnh chỉ tiêu:

```powershell
python dataset_academic_kpi/generate_data.py
```
Thuật toán phân phối có ràng buộc (Bounded Value Constraint Solver) đảm bảo giá trị `AVG()` luôn khớp đúng chỉ tiêu sau khi làm tròn.
