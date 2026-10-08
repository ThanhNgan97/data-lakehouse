-- ============================================================================
-- SCRIPT NẠP DỮ LIỆU TỪ CSV VÀO CƠ SỞ DỮ LIỆU POSTGRESQL
-- ============================================================================

-- Cách 1: Sử dụng lệnh COPY trực tiếp trên máy chủ / Docker container (Khuyên dùng)
COPY dm_don_vi_dao_tao 
FROM '/tmp/dataset_academic_kpi/data/dm_don_vi_dao_tao.csv' 
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY lop_hoc_phan 
FROM '/tmp/dataset_academic_kpi/data/lop_hoc_phan.csv' 
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY tien_do_giang_day 
FROM '/tmp/dataset_academic_kpi/data/tien_do_giang_day.csv' 
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY diem_danh_lop_hp 
FROM '/tmp/dataset_academic_kpi/data/diem_danh_lop_hp.csv' 
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY tien_do_nhap_diem 
FROM '/tmp/dataset_academic_kpi/data/tien_do_nhap_diem.csv' 
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY doi_lich_giang_day 
FROM '/tmp/dataset_academic_kpi/data/doi_lich_giang_day.csv' 
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

COPY khao_sat_sinh_vien 
FROM '/tmp/dataset_academic_kpi/data/khao_sat_sinh_vien.csv' 
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Kiểm tra số lượng bản ghi đã nạp
SELECT 'dm_don_vi_dao_tao' AS ten_bang, COUNT(*) AS tong_so_dong FROM dm_don_vi_dao_tao
UNION ALL
SELECT 'lop_hoc_phan', COUNT(*) FROM lop_hoc_phan
UNION ALL
SELECT 'tien_do_giang_day', COUNT(*) FROM tien_do_giang_day
UNION ALL
SELECT 'diem_danh_lop_hp', COUNT(*) FROM diem_danh_lop_hp
UNION ALL
SELECT 'tien_do_nhap_diem', COUNT(*) FROM tien_do_nhap_diem
UNION ALL
SELECT 'doi_lich_giang_day', COUNT(*) FROM doi_lich_giang_day
UNION ALL
SELECT 'khao_sat_sinh_vien', COUNT(*) FROM khao_sat_sinh_vien;
