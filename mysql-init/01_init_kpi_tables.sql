-- MySQL OLTP seed for CUSC KPI integration
-- Purpose: Day 1 of feature/mysql-lakehouse-integration
-- IMPORTANT:
--   * The KPI catalog and unit codes are based on the current Silver dataset.
--   * Q2/2026 rows below are technical/demo seed data copied from the current KPI values
--     to validate the MySQL -> Bronze -> Silver -> Gold pipeline.
--   * They are not claimed to be official Q2/2026 business results.

CREATE DATABASE IF NOT EXISTS cusc_kpi_operational
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE cusc_kpi_operational;

SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS don_vi (
    ma_don_vi VARCHAR(20) NOT NULL,
    ten_don_vi VARCHAR(255) NOT NULL,
    truong_don_vi VARCHAR(255) NULL,
    PRIMARY KEY (ma_don_vi)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS muc_tieu_kpi (
    ma_chi_tieu VARCHAR(100) NOT NULL,
    noi_dung VARCHAR(1000) NOT NULL,
    dinh_ky VARCHAR(50) NULL,
    PRIMARY KEY (ma_chi_tieu)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ket_qua_danh_gia (
    ma_chi_tieu VARCHAR(100) NOT NULL,
    ma_don_vi VARCHAR(20) NOT NULL,
    quy_danh_gia VARCHAR(20) NOT NULL,
    muc_dang_ky VARCHAR(255) NULL,
    muc_dat VARCHAR(1000) NULL,
    ket_qua_he_thong VARCHAR(50) NULL,
    nguyen_nhan TEXT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ma_chi_tieu, ma_don_vi, quy_danh_gia),
    CONSTRAINT fk_ket_qua_muc_tieu
        FOREIGN KEY (ma_chi_tieu)
        REFERENCES muc_tieu_kpi(ma_chi_tieu),
    CONSTRAINT fk_ket_qua_don_vi
        FOREIGN KEY (ma_don_vi)
        REFERENCES don_vi(ma_don_vi)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- 6 CUSC units already used by the current Silver/Gold pipeline.
INSERT INTO don_vi (ma_don_vi, ten_don_vi, truong_don_vi)
VALUES
    ('HT',   'Phòng Hạ tầng - An ninh mạng', NULL),
    ('PM',   'Trung tâm Phần mềm', NULL),
    ('QTCL', 'Bộ phận Quản trị Chất lượng', NULL),
    ('RD',   'Phòng Nghiên cứu & Phát triển', NULL),
    ('VP',   'Văn phòng', NULL),
    ('ĐT',   'Phòng Đào tạo', NULL)
ON DUPLICATE KEY UPDATE
    ten_don_vi = VALUES(ten_don_vi);

-- KPI catalog derived from the current Silver dataset.
INSERT INTO muc_tieu_kpi (ma_chi_tieu, noi_dung, dinh_ky)
VALUES
    ('HT-MT01', 'Dữ liệu được backup đầy đủ và đúng hạn', 'Quý'),
    ('HT-MT02', 'Cấp phát tài nguyên email và tài khoản hệ thống cho VCNLĐ chính xác và đúng hạn', 'Quý'),
    ('HT-MT03', 'Đảm bảo kịp thời có giải pháp phòng chống, khắc phục các sự cố bảo mật của hệ thống thông tin.', 'Quý'),
    ('HT-MT04', 'Mức độ hài lòng của khách hàng nội bộ về tính ổn định của mạng nội bộ.', 'Quý'),
    ('HT-MT05', 'Mức độ hài lòng của khách hàng nội bộ về hoạt động hỗ trợ của Phòng QTANM', '6 tháng'),
    ('PM-MT01', 'Tỷ lệ dự án hoàn thành đúng hạn theo thỏa thuận với khách hàng', 'Quý'),
    ('PM-MT03', 'Sản phẩm phần mềm đáp ứng yêu cầu chức năng của Khách hàng', 'Quý'),
    ('PM-MT04', 'Mức độ hài lòng của khách hàng', '6 tháng'),
    ('PM-MT05', 'Tỷ lệ trả lời yêu cầu của khách hàng trong vòng 48 giờ', 'Quý'),
    ('PM-MT11', 'Tỷ lệ sửa lỗi của dự án', 'Quý'),
    ('PM-MT12', 'Dự án phần mềm có thực hiện kiểm định bảo mật', 'Quý'),
    ('QTCL-MT01', 'Đảm bảo hoạt động Đánh giá nội bộ đầy đủ các điều khoản chính theo quy định của ISO 9001:2015 và ISO 27001:2022', '6 tháng'),
    ('QTCL-MT02', 'VC-NLĐ được đào tạo về quy trình làm việc của các đơn vị', '6 tháng'),
    ('QTCL-MT03', 'Thực hiện kế hoạch cải tiến tài liệu chất lượng của các đơn vị', '6 tháng'),
    ('RD-MT01', 'Nghiệm thu kết quả nghiên cứu (Công nghệ mới, giải pháp mới, cải tiến sản phẩm của Trung tâm,…)', 'Quý'),
    ('RD-MT02', 'Nâng cao sự nhận diện của Trung tâm trên các diễn đàn học thuật (báo cáo tham luận/ bài báo khoa học)', 'Năm'),
    ('RD-MT03', 'Đăng ký bảo hộ quyền sở hữu trí tuệ cho kết quả nghiên cứu: quyền tác giả/ sáng chế/ giải pháp hữu ích', 'Năm'),
    ('RD-MT04', 'Đăng ký đề xuất/ thuyết minh đề tài NCKH cấp tỉnh/ Bộ', 'Năm'),
    ('VP-MT01', 'Chất lượng dịch vụ hỗ trợ học tập cho sinh viên.', 'Quý'),
    ('VP-MT04', 'Thực hiện mua sắm, sửa chữa kịp thời hạn (thiết bị/VPP).', 'Quý'),
    ('VP-MT12', 'Mức độ đáp ứng hồ sơ ứng viên dài hạn, vị trí Lập trình viên.', '6 tháng'),
    ('VP-MT14', 'Chất lượng cơ sở vật chất và cảnh quan môi trường làm việc', '6 tháng'),
    ('VP-MT15', 'VC-NLĐ tuyển mới được đào tạo về nhận thức', '6 tháng'),
    ('VP-MT16', 'Ký thỏa thuận bảo mật khi tiếp nhận VC-NLĐ tuyển mới', '6 tháng'),
    ('ĐT-MT01', 'Máy tính và trang thiết bị ở trạng thái sẵn dùng phục vụ giảng dạy và thi cử', 'Quý'),
    ('ĐT-MT02', 'Tỷ lệ sinh viên thi đạt', 'Quý'),
    ('ĐT-MT03', 'Tỷ lệ sinh viên bỏ học (drop-out)', 'Quý'),
    ('ĐT-MT04', 'Tỷ lệ tốt nghiệp của sinh viên dài hạn', 'Quý'),
    ('ĐT-MT06', 'Tài liệu giáo trình sẵn sàng cho môn học bắt đầu', 'Quý'),
    ('ĐT-MT07', 'Nội dung chương trình đào tạo', 'Quý'),
    ('ĐT-MT09', 'Các dịch vụ hỗ trợ hoạt động đào tạo.', 'Quý'),
    ('ĐT-MT11', 'Tỷ lệ SV tốt nghiệp có việc làm hoặc học lên tiếp (Sau 6 tháng kể từ khi tốt nghiệp)', 'Quý'),
    ('ĐT-MT12', 'Đạt chuẩn giảng dạy (Thi chứng nhận chuyên môn + Đánh giá giảng thử)', 'Quý'),
    ('ĐT-MT13', 'Điểm GPA trung bình của giáo viên', 'Quý')
ON DUPLICATE KEY UPDATE
    noi_dung = VALUES(noi_dung),
    dinh_ky = VALUES(dinh_ky);

-- 34 technical/demo Q2/2026 operational rows.
-- Raw textual values are intentionally preserved. Numeric normalization belongs to PySpark/Silver.
INSERT INTO ket_qua_danh_gia
    (ma_chi_tieu, ma_don_vi, quy_danh_gia, muc_dang_ky, muc_dat, ket_qua_he_thong, nguyen_nhan)
VALUES
    ('HT-MT01','HT','Q2/2026','100%','100%','ĐẠT',NULL),
    ('HT-MT02','HT','Q2/2026','100%','100%','ĐẠT',NULL),
    ('HT-MT03','HT','Q2/2026','100%','Không phát sinh sự cố','ĐẠT',NULL),
    ('HT-MT04','HT','Q2/2026','7,5','9.5','ĐẠT',NULL),
    ('HT-MT05','HT','Q2/2026','8,5','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),

    ('PM-MT01','PM','Q2/2026','80%','100%','ĐẠT',NULL),
    ('PM-MT03','PM','Q2/2026','80%','100%','ĐẠT',NULL),
    ('PM-MT04','PM','Q2/2026','80%','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('PM-MT05','PM','Q2/2026','93%','100%','ĐẠT',NULL),
    ('PM-MT11','PM','Q2/2026','Tổng lỗi phát hiện được sửa: 70% / Tổng lỗi crash được sửa: 90%','99,5% (Tổng lỗi phát hiện được sửa) / 100% (Tổng lỗi crash được sửa)','ĐẠT',NULL),
    ('PM-MT12','PM','Q2/2026','100%','100%','ĐẠT',NULL),

    ('QTCL-MT01','QTCL','Q2/2026','100%','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('QTCL-MT02','QTCL','Q2/2026','100%','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('QTCL-MT03','QTCL','Q2/2026','80%','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),

    ('RD-MT01','RD','Q2/2026','90%','100%','ĐẠT',NULL),
    ('RD-MT02','RD','Q2/2026','2','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('RD-MT03','RD','Q2/2026','2','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('RD-MT04','RD','Q2/2026','2','4','ĐẠT',NULL),

    ('VP-MT01','VP','Q2/2026','3,5','3,69','ĐẠT',NULL),
    ('VP-MT04','VP','Q2/2026','95%','100%','ĐẠT',NULL),
    ('VP-MT12','VP','Q2/2026','100%','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('VP-MT14','VP','Q2/2026','7,5','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('VP-MT15','VP','Q2/2026','100%','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),
    ('VP-MT16','VP','Q2/2026','100%','Chưa đến kỳ đánh giá','CHƯA ĐẾN KỲ ĐÁNH GIÁ',NULL),

    ('ĐT-MT01','ĐT','Q2/2026','85%','86,17%','ĐẠT',NULL),
    ('ĐT-MT02','ĐT','Q2/2026','85%','91,09%','ĐẠT',NULL),
    ('ĐT-MT03','ĐT','Q2/2026','15%','18%','KHÔNG ĐẠT',NULL),
    ('ĐT-MT04','ĐT','Q2/2026','70%','84%','ĐẠT',NULL),
    ('ĐT-MT06','ĐT','Q2/2026','95%','100,00%','ĐẠT',NULL),
    ('ĐT-MT07','ĐT','Q2/2026','90%','91,89%','ĐẠT',NULL),
    ('ĐT-MT09','ĐT','Q2/2026','85%','93,17%','ĐẠT',NULL),
    ('ĐT-MT11','ĐT','Q2/2026','90%','100,00%','ĐẠT',NULL),
    ('ĐT-MT12','ĐT','Q2/2026','90%','81,82%','KHÔNG ĐẠT',NULL),
    ('ĐT-MT13','ĐT','Q2/2026','3,5','3,80','ĐẠT',NULL)
ON DUPLICATE KEY UPDATE
    muc_dang_ky = VALUES(muc_dang_ky),
    muc_dat = VALUES(muc_dat),
    ket_qua_he_thong = VALUES(ket_qua_he_thong),
    nguyen_nhan = VALUES(nguyen_nhan),
    updated_at = CURRENT_TIMESTAMP;

-- Validation summary for first-run logs / manual execution.
SELECT 'don_vi' AS bang, COUNT(*) AS so_dong FROM don_vi
UNION ALL
SELECT 'muc_tieu_kpi', COUNT(*) FROM muc_tieu_kpi
UNION ALL
SELECT 'ket_qua_danh_gia', COUNT(*) FROM ket_qua_danh_gia;
