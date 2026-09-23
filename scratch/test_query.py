# -*- coding: utf-8 -*-
import sqlalchemy

engine = sqlalchemy.create_engine('trino://admin@trino:8080/lakehouse')

queries = {
    "KPI 1 (Tổng lớp HP)": "SELECT SUM(tong_lop_hp) FROM gold.kpi_api_teaching_summary",
    "KPI 2 (Đúng tiến độ)": "SELECT AVG(tb_dung_tien_do_pct) FROM gold.kpi_api_teaching_summary",
    "KPI 3 (Hiện diện)": "SELECT AVG(tb_hien_dien_pct) FROM gold.kpi_api_teaching_summary",
    "KPI 4 (Nhập điểm)": "SELECT AVG(tb_nhap_diem_pct) FROM gold.kpi_api_teaching_summary",
    "KPI 5 (Đổi lịch)": "SELECT SUM(tong_doi_lich) FROM gold.kpi_api_teaching_summary",
    "Chart 6 (Tỷ lệ đúng tiến độ)": "SELECT don_vi_dao_tao, AVG(tb_dung_tien_do_pct) FROM gold.kpi_api_teaching_summary GROUP BY don_vi_dao_tao",
    "Chart 7 (Hiện diện và Nhập điểm)": "SELECT don_vi_dao_tao, AVG(tb_hien_dien_pct), AVG(tb_nhap_diem_pct) FROM gold.kpi_api_teaching_summary GROUP BY don_vi_dao_tao",
    "Chart 8 (Số lần đổi lịch)": "SELECT don_vi_dao_tao, SUM(tong_doi_lich) FROM gold.kpi_api_teaching_summary GROUP BY don_vi_dao_tao",
    "Chart 9 (Phản hồi SV)": "SELECT don_vi_dao_tao, AVG(TRY_CAST(SPLIT_PART(diem_phan_hoi_sv, '/', 1) AS DOUBLE)) FROM gold.kpi_api_teaching_summary GROUP BY don_vi_dao_tao",
    "Chart 10 (Phân loại đánh giá)": "SELECT danh_gia, COUNT(don_vi_dao_tao) FROM gold.kpi_api_teaching_summary GROUP BY danh_gia",
    "Chart 11 (Chi tiết theo đơn vị)": "SELECT don_vi_dao_tao, SUM(tong_lop_hp), AVG(tb_dung_tien_do_pct), AVG(tb_hien_dien_pct), AVG(tb_nhap_diem_pct), SUM(tong_doi_lich) FROM gold.kpi_api_teaching_summary GROUP BY don_vi_dao_tao"
}

for name, q in queries.items():
    try:
        res = engine.execute(q).fetchall()
        print(f"✅ {name}: OK ({len(res)} rows)")
    except Exception as e:
        print(f"❌ {name}: LỖI - {e}")
