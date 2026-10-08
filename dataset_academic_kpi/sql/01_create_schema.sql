-- ============================================================================
-- HỆ THỐNG GIÁM SÁT TIẾN ĐỘ & CHẤT LƯỢNG ĐÀO TẠO (ACADEMIC OPERATION CENTER - IOC)
-- CƠ SỞ DỮ LIỆU: PostgreSQL 14+
-- TẬP LỆNH DDL KHỞI TẠO BẢNG & RÀNG BUỘC TOÀN VẸN
-- ============================================================================

-- Xóa bảng cũ nếu tồn tại theo thứ tự phụ thuộc khóa ngoại
DROP TABLE IF EXISTS khao_sat_sinh_vien CASCADE;
DROP TABLE IF EXISTS doi_lich_giang_day CASCADE;
DROP TABLE IF EXISTS tien_do_nhap_diem CASCADE;
DROP TABLE IF EXISTS diem_danh_lop_hp CASCADE;
DROP TABLE IF EXISTS tien_do_giang_day CASCADE;
DROP TABLE IF EXISTS lop_hoc_phan CASCADE;
DROP TABLE IF EXISTS dm_don_vi_dao_tao CASCADE;

-- ----------------------------------------------------------------------------
-- 1. DANH MỤC ĐƠN VỊ ĐÀO TẠO (TRƯỜNG / KHOA / VIỆN)
-- ----------------------------------------------------------------------------
CREATE TABLE dm_don_vi_dao_tao (
    ma_don_vi           VARCHAR(20)     PRIMARY KEY,
    ten_don_vi          VARCHAR(150)    NOT NULL,
    loai_don_vi         VARCHAR(50)     NOT NULL, -- 'Trường', 'Khoa', 'Viện'
    thu_tu_hien_thi     INT             NOT NULL UNIQUE
);

COMMENT ON TABLE dm_don_vi_dao_tao IS 'Danh mục các đơn vị đào tạo trực thuộc đại học';
COMMENT ON COLUMN dm_don_vi_dao_tao.ma_don_vi IS 'Mã định danh đơn vị (BK, KHTN, CNTT, ...)';
COMMENT ON COLUMN dm_don_vi_dao_tao.ten_don_vi IS 'Tên đầy đủ của đơn vị đào tạo';
COMMENT ON COLUMN dm_don_vi_dao_tao.thu_tu_hien_thi IS 'Thứ tự sắp xếp hiển thị trên Dashboard IOC';

-- ----------------------------------------------------------------------------
-- 2. DANH SÁCH LỚP HỌC PHẦN TRONG HỌC KỲ
-- ----------------------------------------------------------------------------
CREATE TABLE lop_hoc_phan (
    ma_lop_hp           VARCHAR(30)     PRIMARY KEY,
    ma_don_vi           VARCHAR(20)     NOT NULL REFERENCES dm_don_vi_dao_tao(ma_don_vi) ON DELETE RESTRICT,
    ma_mon_hoc          VARCHAR(20)     NOT NULL,
    ten_mon_hoc         VARCHAR(150)    NOT NULL,
    so_tin_chi          INT             NOT NULL CHECK (so_tin_chi > 0),
    so_sinh_vien        INT             NOT NULL CHECK (so_sinh_vien >= 0),
    giang_vien          VARCHAR(100)    NOT NULL,
    hoc_ky              VARCHAR(10)     NOT NULL DEFAULT 'HK1',
    nam_hoc             VARCHAR(20)     NOT NULL DEFAULT '2025-2026'
);

CREATE INDEX idx_lhp_don_vi ON lop_hoc_phan(ma_don_vi);
CREATE INDEX idx_lhp_hoc_ky_nam ON lop_hoc_phan(hoc_ky, nam_hoc);

COMMENT ON TABLE lop_hoc_phan IS 'Bảng danh sách lớp học phần được mở trong học kỳ';

-- ----------------------------------------------------------------------------
-- 3. THEO DÕI TIẾN ĐỘ GIẢNG DẠY (ĐÚNG TIẾN ĐỘ)
-- ----------------------------------------------------------------------------
CREATE TABLE tien_do_giang_day (
    ma_lop_hp           VARCHAR(30)     PRIMARY KEY REFERENCES lop_hoc_phan(ma_lop_hp) ON DELETE CASCADE,
    so_tiet_ke_hoach    INT             NOT NULL CHECK (so_tiet_ke_hoach > 0),
    so_tiet_thuc_day    INT             NOT NULL CHECK (so_tiet_thuc_day >= 0),
    ty_le_dung_tien_do  NUMERIC(5, 2)   NOT NULL CHECK (ty_le_dung_tien_do BETWEEN 0 AND 100),
    trang_thai_tien_do  VARCHAR(50)     NOT NULL
);

COMMENT ON TABLE tien_do_giang_day IS 'Tiến độ thực hiện kế hoạch giảng dạy theo đề cương';
COMMENT ON COLUMN tien_do_giang_day.ty_le_dung_tien_do IS 'Tỷ lệ % hoàn thành đúng số tiết/tuần quy định';

-- ----------------------------------------------------------------------------
-- 4. THEO DÕI ĐIỂM DANH SINH VIÊN (HIỆN DIỆN)
-- ----------------------------------------------------------------------------
CREATE TABLE diem_danh_lop_hp (
    ma_lop_hp           VARCHAR(30)     PRIMARY KEY REFERENCES lop_hoc_phan(ma_lop_hp) ON DELETE CASCADE,
    tong_luot_diem_danh INT             NOT NULL CHECK (tong_luot_diem_danh >= 0),
    so_luot_co_mat      INT             NOT NULL CHECK (so_luot_co_mat >= 0),
    ty_le_hien_dien     NUMERIC(5, 2)   NOT NULL CHECK (ty_le_hien_dien BETWEEN 0 AND 100)
);

COMMENT ON TABLE diem_danh_lop_hp IS 'Thống kê điểm danh và tỷ lệ hiện diện/chuyên cần của lớp học phần';

-- ----------------------------------------------------------------------------
-- 5. THEO DÕI TIẾN ĐỘ NHẬP ĐIỂM CỦA GIẢNG VIÊN (NHẬP ĐIỂM)
-- ----------------------------------------------------------------------------
CREATE TABLE tien_do_nhap_diem (
    ma_lop_hp           VARCHAR(30)     PRIMARY KEY REFERENCES lop_hoc_phan(ma_lop_hp) ON DELETE CASCADE,
    so_cot_diem_quy_dinh INT            NOT NULL DEFAULT 3,
    so_cot_diem_da_nhap INT             NOT NULL CHECK (so_cot_diem_da_nhap >= 0),
    ty_le_nhap_diem     NUMERIC(5, 2)   NOT NULL CHECK (ty_le_nhap_diem BETWEEN 0 AND 100),
    han_chot_nhap_diem  DATE            NOT NULL,
    ngay_hoan_thanh     DATE,
    trang_thai_nhap_diem VARCHAR(50)    NOT NULL
);

COMMENT ON TABLE tien_do_nhap_diem IS 'Tiến độ cập nhật điểm bộ phận và điểm thi kết thúc học phần';

-- ----------------------------------------------------------------------------
-- 6. THEO DÕI ĐỔI LỊCH GIẢNG DẠY (ĐỔI LỊCH)
-- ----------------------------------------------------------------------------
CREATE TABLE doi_lich_giang_day (
    ma_yeu_cau          VARCHAR(30)     PRIMARY KEY,
    ma_lop_hp           VARCHAR(30)     NOT NULL REFERENCES lop_hoc_phan(ma_lop_hp) ON DELETE CASCADE,
    ngay_yeu_cau        DATE            NOT NULL,
    thoi_gian_cu        TIMESTAMP       NOT NULL,
    thoi_gian_moi       TIMESTAMP       NOT NULL,
    ly_do               VARCHAR(255)    NOT NULL,
    trang_thai          VARCHAR(50)     NOT NULL DEFAULT 'Đã duyệt'
);

CREATE INDEX idx_doi_lich_lop_hp ON doi_lich_giang_day(ma_lop_hp);

COMMENT ON TABLE doi_lich_giang_day IS 'Nhật ký các đơn xin nghỉ dạy/đổi phòng/bù giờ học phần';

-- ----------------------------------------------------------------------------
-- 7. KHẢO SÁT Ý KIẾN PHẢN HỒI SINH VIÊN (PHẢN HỒI SV)
-- ----------------------------------------------------------------------------
CREATE TABLE khao_sat_sinh_vien (
    ma_khao_sat         VARCHAR(30)     PRIMARY KEY,
    ma_lop_hp           VARCHAR(30)     NOT NULL REFERENCES lop_hoc_phan(ma_lop_hp) ON DELETE CASCADE,
    so_sv_khao_sat      INT             NOT NULL CHECK (so_sv_khao_sat >= 0),
    diem_danh_gia       NUMERIC(3, 2)   NOT NULL CHECK (diem_danh_gia BETWEEN 1.00 AND 5.00),
    muc_do_hai_long     VARCHAR(50)     NOT NULL
);

CREATE INDEX idx_khao_sat_lop_hp ON khao_sat_sinh_vien(ma_lop_hp);

COMMENT ON TABLE khao_sat_sinh_vien IS 'Điểm đánh giá chất lượng giảng dạy từ sinh viên (thang điểm 5)';
