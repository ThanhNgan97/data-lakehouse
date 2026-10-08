-- ============================================================================
-- VIEW TỔNG HỢP BÁO CÁO GIÁM SÁT TIẾN ĐỘ & CHẤT LƯỢNG ĐÀO TẠO
-- KHỚP CHÍNH XÁC 100% CÁC CHỈ SỐ DASHBOARD IOC
-- ============================================================================

CREATE OR REPLACE VIEW v_giam_sat_tien_do_dao_tao AS
WITH agg_lop AS (
    SELECT 
        lhp.ma_don_vi,
        COUNT(lhp.ma_lop_hp)                    AS lop_hp,
        ROUND(AVG(td.ty_le_dung_tien_do), 1)    AS dung_tien_do_num,
        ROUND(AVG(dd.ty_le_hien_dien), 1)       AS hien_dien_num,
        ROUND(AVG(nd.ty_le_nhap_diem), 1)       AS nhap_diem_num,
        ROUND(AVG(ks.diem_danh_gia), 2)         AS phan_hoi_sv_num
    FROM lop_hoc_phan lhp
    LEFT JOIN tien_do_giang_day td ON lhp.ma_lop_hp = td.ma_lop_hp
    LEFT JOIN diem_danh_lop_hp dd ON lhp.ma_lop_hp = dd.ma_lop_hp
    LEFT JOIN tien_do_nhap_diem nd ON lhp.ma_lop_hp = nd.ma_lop_hp
    LEFT JOIN khao_sat_sinh_vien ks ON lhp.ma_lop_hp = ks.ma_lop_hp
    GROUP BY lhp.ma_don_vi
),
agg_doi_lich AS (
    SELECT 
        lhp.ma_don_vi,
        COUNT(dl.ma_yeu_cau)                    AS doi_lich_count
    FROM doi_lich_giang_day dl
    JOIN lop_hoc_phan lhp ON dl.ma_lop_hp = lhp.ma_lop_hp
    GROUP BY lhp.ma_don_vi
)
SELECT 
    dv.thu_tu_hien_thi                                              AS "STT",
    dv.ten_don_vi                                                   AS "ĐƠN VỊ ĐÀO TẠO",
    al.lop_hp                                                       AS "LỚP HP",
    TO_CHAR(al.dung_tien_do_num, 'FM990.0') || '%'                  AS "ĐÚNG TIẾN ĐỘ",
    TO_CHAR(al.hien_dien_num, 'FM990.0') || '%'                     AS "HIỆN DIỆN",
    TO_CHAR(al.nhap_diem_num, 'FM990.0') || '%'                     AS "NHẬP ĐIỂM",
    COALESCE(adl.doi_lich_count, 0)                                 AS "ĐỔI LỊCH",
    TO_CHAR(al.phan_hoi_sv_num, 'FM990.00') || '/5'                 AS "PHẢN HỒI SV",
    CASE 
        WHEN al.dung_tien_do_num >= 98.5 AND al.phan_hoi_sv_num >= 4.60 THEN 'Xuất sắc'
        WHEN al.dung_tien_do_num >= 97.0 THEN 'Tốt'
        WHEN al.dung_tien_do_num >= 96.0 THEN 'Đạt'
        WHEN al.dung_tien_do_num >= 93.0 THEN 'Cần theo dõi'
        ELSE 'Cần cải thiện'
    END                                                             AS "ĐÁNH GIÁ"
FROM dm_don_vi_dao_tao dv
JOIN agg_lop al ON dv.ma_don_vi = al.ma_don_vi
LEFT JOIN agg_doi_lich adl ON dv.ma_don_vi = adl.ma_don_vi
ORDER BY dv.thu_tu_hien_thi;

-- Kiểm tra truy vấn trực tiếp từ View
SELECT * FROM v_giam_sat_tien_do_dao_tao;
