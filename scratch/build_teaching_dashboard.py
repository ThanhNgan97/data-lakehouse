# -*- coding: utf-8 -*-
"""
build_teaching_dashboard.py
------------------------------------------------------------
Script tự động khởi tạo Dashboard "TIẾN TRÌNH ĐÀO TẠO" trên Apache Superset
Dùng dữ liệu thật từ Trino (Bảng lakehouse.gold.kpi_api_teaching_summary).
Layout rộng rãi, thông thoáng giúp các số liệu trên đầu cột hiển thị 100% rõ ràng.
------------------------------------------------------------
"""
import sys
import json

from superset.app import create_app
app = create_app()

with app.app_context():
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.models.core import Database

    print("🔍 Tìm kiếm Dataset 'kpi_api_teaching_summary'...")
    dataset = db.session.query(SqlaTable).filter(SqlaTable.table_name == 'kpi_api_teaching_summary').first()
    
    if not dataset:
        database = db.session.query(Database).filter(Database.database_name.ilike('%trino%')).first()
        if not database:
            database = db.session.query(Database).first()
        
        if database:
            dataset = SqlaTable(
                table_name='kpi_api_teaching_summary',
                schema='gold',
                database_id=database.id
            )
            db.session.add(dataset)
            db.session.commit()
            try:
                dataset.fetch_metadata()
            except Exception:
                pass
            print(f"✅ Đã tạo Dataset kpi_api_teaching_summary (ID={dataset.id})")
        else:
            print("❌ Không tìm thấy Database Trino trong Superset.")
            sys.exit(1)

    dataset.main_dttm_col = "thoi_gian_dong_goi_gold"
    for col in dataset.columns:
        if col.column_name in ["thoi_gian_dong_goi_gold", "thoi_gian_ingest_silver"]:
            col.is_dttm = True
    db.session.commit()

    datasource_id = dataset.id
    datasource_type = 'table'
    datasource_name = f"{dataset.id}__table"

    # Xóa các Slice cũ
    existing_slices = db.session.query(Slice).filter(Slice.slice_name.like('TP - %')).all()
    for s in existing_slices:
        db.session.delete(s)
    db.session.commit()

    def make_simple_metric(col_name, agg="SUM", label=None):
        return {
            "expressionType": "SIMPLE",
            "column": {"column_name": col_name},
            "aggregate": agg,
            "label": label or f"{agg}({col_name})"
        }

    def make_sql_metric(sql_expr, label):
        return {
            "expressionType": "SQL",
            "sqlExpression": sql_expr,
            "label": label
        }

    base_params = {
        "datasource": datasource_name,
        "granularity_sqla": "thoi_gian_dong_goi_gold",
        "time_range": "No filter",
    }

    # 1. KPI - Tổng lớp HP
    p1 = {**base_params, "viz_type": "big_number_total", "metric": make_simple_metric("tong_lop_hp", "SUM", "Tổng lớp HP"), "subheader": "Tổng lớp HP", "header_font_size": 0.35, "y_axis_format": ",d"}
    s1 = Slice(slice_name="TP - KPI Tổng lớp HP", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p1))
    db.session.add(s1)

    # 2. KPI - Đúng tiến độ
    p2 = {**base_params, "viz_type": "big_number_total", "metric": make_simple_metric("tb_dung_tien_do_pct", "AVG", "Đúng tiến độ"), "subheader": "Đúng tiến độ", "y_axis_format": ".1f", "header_font_size": 0.35}
    s2 = Slice(slice_name="TP - KPI Đúng tiến độ", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p2))
    db.session.add(s2)

    # 3. KPI - Hiện diện
    p3 = {**base_params, "viz_type": "big_number_total", "metric": make_simple_metric("tb_hien_dien_pct", "AVG", "Hiện diện"), "subheader": "Hiện diện", "y_axis_format": ".1f", "header_font_size": 0.35}
    s3 = Slice(slice_name="TP - KPI Hiện diện", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p3))
    db.session.add(s3)

    # 4. KPI - Nhập điểm
    p4 = {**base_params, "viz_type": "big_number_total", "metric": make_simple_metric("tb_nhap_diem_pct", "AVG", "Nhập điểm"), "subheader": "Nhập điểm", "y_axis_format": ".1f", "header_font_size": 0.35}
    s4 = Slice(slice_name="TP - KPI Nhập điểm", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p4))
    db.session.add(s4)

    # 5. KPI - Đổi lịch
    p5 = {**base_params, "viz_type": "big_number_total", "metric": make_simple_metric("tong_doi_lich", "SUM", "Đổi lịch"), "subheader": "Đổi lịch", "y_axis_format": ",d", "header_font_size": 0.35}
    s5 = Slice(slice_name="TP - KPI Đổi lịch", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p5))
    db.session.add(s5)

    # 6. TỶ LỆ ĐÚNG TIẾN ĐỘ (Horizontal Bar Chart)
    p6 = {**base_params,
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "don_vi_dao_tao",
        "metrics": [make_simple_metric("tb_dung_tien_do_pct", "AVG", "Tỷ lệ đúng tiến độ (%)")],
        "groupby": [],
        "orientation": "horizontal",
        "bar_stacked": False,
        "show_value": True,
        "show_legend": False,
        "y_axis_bounds": [0, 105],
        "y_axis_format": ".1f",
        "color_scheme": "googleCategory20",
        "label_colors": {
            "Bách khoa": "#3B82F6",
            "KH Tự nhiên": "#10B981",
            "Kinh tế": "#8B5CF6",
            "Ngoại ngữ": "#F59E0B",
            "Sư phạm": "#06B6D4",
            "CNTT": "#6366F1",
            "Nông nghiệp": "#EC4899",
            "Thủy sản": "#14B8A6",
            "KHXH&NV": "#EAB308",
            "Viện CNSH&TP": "#84CC16"
        }
    }
    s6 = Slice(slice_name="TP - Tỷ lệ đúng tiến độ", viz_type="echarts_timeseries_bar", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p6))
    db.session.add(s6)

    # 7. HIỆN DIỆN VÀ NHẬP ĐIỂM (Grouped Bar Chart - CỘT GIÃN RỘNG RÃI, KHÔNG CHỒNG CHÉO SỐ)
    p7 = {**base_params,
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "don_vi_dao_tao",
        "metrics": [
            make_simple_metric("tb_hien_dien_pct", "AVG", "Hiện diện (%)"),
            make_simple_metric("tb_nhap_diem_pct", "AVG", "Nhập điểm (%)")
        ],
        "groupby": [],
        "bar_stacked": False,
        "show_value": True,
        "show_legend": True,
        "y_axis_bounds": [0, 110],  # Nâng trần trục Y lên 110% để số trên đầu cột hiển thị cực kỳ thoáng
        "y_axis_format": ".1f",
        "color_scheme": "googleCategory20",
        "label_colors": {
            "Hiện diện (%)": "#2563EB",
            "Nhập điểm (%)": "#9333EA"
        }
    }
    s7 = Slice(slice_name="TP - Hiện diện và Nhập điểm", viz_type="echarts_timeseries_bar", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p7))
    db.session.add(s7)

    # 8. SỐ LẦN ĐỔI LỊCH (Bar Chart)
    p8 = {**base_params,
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "don_vi_dao_tao",
        "metrics": [make_simple_metric("tong_doi_lich", "SUM", "Số lần")],
        "groupby": [],
        "bar_stacked": False,
        "color_scheme": "googleCategory20",
        "show_value": True,
        "show_legend": False,
    }
    s8 = Slice(slice_name="TP - Số lần đổi lịch", viz_type="echarts_timeseries_bar", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p8))
    db.session.add(s8)

    # 9. PHẢN HỒI SV (Bar Chart)
    sql_diem_phan_hoi = "AVG(TRY_CAST(REPLACE(SPLIT_PART(diem_phan_hoi_sv, '/', 1), ',', '.') AS DOUBLE))"
    p9 = {**base_params,
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "don_vi_dao_tao",
        "metrics": [make_sql_metric(sql_diem_phan_hoi, "Điểm trung bình")],
        "groupby": [],
        "bar_stacked": False,
        "color_scheme": "d3Category10",
        "show_value": True,
        "show_legend": False,
        "y_axis_bounds": [0, 5],
        "y_axis_format": ".2f",
    }
    s9 = Slice(slice_name="TP - Phản hồi SV", viz_type="echarts_timeseries_bar", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p9))
    db.session.add(s9)

    # 10. PHẦN LOẠI ĐÁNH GIÁ (Donut Pie Chart)
    p10 = {**base_params,
        "viz_type": "pie",
        "groupby": ["danh_gia"],
        "metric": make_simple_metric("don_vi_dao_tao", "COUNT", "Số lượng đơn vị"),
        "is_donut": True,
        "color_scheme": "googleCategory20",
        "show_labels": True,
        "show_legend": True,
        "label_colors": {
            "Cần cải thiện": "#EF4444",
            "Cần theo dõi": "#F59E0B",
            "Đạt": "#3B82F6",
            "Tốt": "#10B981",
            "Xuất sắc": "#8B5CF6"
        }
    }
    s10 = Slice(slice_name="TP - Phân loại đánh giá", viz_type="pie", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p10))
    db.session.add(s10)

    # 11. CHI TIẾT THEO ĐƠN VỊ (Table Chart)
    p11 = {**base_params,
        "viz_type": "table",
        "groupby": ["don_vi_dao_tao"],
        "metrics": [
            make_simple_metric("tong_lop_hp", "SUM", "Lớp HP"),
            make_simple_metric("tb_dung_tien_do_pct", "AVG", "Đúng tiến độ (%)"),
            make_simple_metric("tb_hien_dien_pct", "AVG", "Hiện diện (%)"),
            make_simple_metric("tb_nhap_diem_pct", "AVG", "Nhập điểm (%)"),
            make_simple_metric("tong_doi_lich", "SUM", "Đổi lịch")
        ],
        "page_length": 10,
        "include_search": True,
        "query_mode": "aggregate"
    }
    s11 = Slice(slice_name="TP - Chi tiết theo đơn vị", viz_type="table", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p11))
    db.session.add(s11)

    db.session.commit()

    all_slices = [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11]

    # Gán Admin user làm owner
    from flask_appbuilder.security.sqla.models import User
    admin_user = db.session.query(User).filter(User.username == 'admin').first()
    for s in all_slices:
        if admin_user and admin_user not in s.owners:
            s.owners.append(admin_user)
    db.session.commit()

    # Xóa Dashboard cũ nếu tồn tại
    existing_dash = db.session.query(Dashboard).filter(
        (Dashboard.dashboard_title == 'TIẾN TRÌNH ĐÀO TẠO') | (Dashboard.slug == 'tien-trinh-dao-tao')
    ).first()
    if existing_dash:
        db.session.delete(existing_dash)
        db.session.commit()

    # Xây dựng Layout Dashboard rộng rãi, các biểu đồ được nới rộng tối đa
    position_json = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "GRID_ID": {"children": ["ROW-1", "ROW-2", "ROW-3", "ROW-4", "ROW-5"], "id": "GRID_ID", "type": "GRID"},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "TIẾN TRÌNH ĐÀO TẠO"}},

        # Row 1: 5 KPI Cards
        "ROW-1": {"children": [f"CHART-{s1.id}", f"CHART-{s2.id}", f"CHART-{s3.id}", f"CHART-{s4.id}", f"CHART-{s5.id}"], "id": "ROW-1", "type": "ROW", "meta": {"background": "BACKGROUND_TRANSPARENT"}},
        
        # Row 2: Tỷ lệ đúng tiến độ (Width 12 - Rộng rãi toàn chiều ngang)
        "ROW-2": {"children": [f"CHART-{s6.id}"], "id": "ROW-2", "type": "ROW", "meta": {"background": "BACKGROUND_TRANSPARENT"}},

        # Row 3: Hiện diện và Nhập điểm (Width 12 - Các cột được nới rộng cực kỳ thông thoáng, số liệu không bị đè)
        "ROW-3": {"children": [f"CHART-{s7.id}"], "id": "ROW-3", "type": "ROW", "meta": {"background": "BACKGROUND_TRANSPARENT"}},

        # Row 4: Đổi lịch (4), Phản hồi SV (4), Phân loại đánh giá (4)
        "ROW-4": {"children": [f"CHART-{s8.id}", f"CHART-{s9.id}", f"CHART-{s10.id}"], "id": "ROW-4", "type": "ROW", "meta": {"background": "BACKGROUND_TRANSPARENT"}},

        # Row 5: Table Chi tiết theo đơn vị (Width 12)
        "ROW-5": {"children": [f"CHART-{s11.id}"], "id": "ROW-5", "type": "ROW", "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    }

    # Width 5 KPI
    kpi_widths = [2, 2, 3, 2, 3]
    for idx, s in enumerate([s1, s2, s3, s4, s5]):
        position_json[f"CHART-{s.id}"] = {"children": [], "id": f"CHART-{s.id}", "type": "CHART", "meta": {"chartId": s.id, "height": 26, "width": kpi_widths[idx], "sliceName": s.slice_name}}

    # Row 2 (s6): width 12
    position_json[f"CHART-{s6.id}"] = {"children": [], "id": f"CHART-{s6.id}", "type": "CHART", "meta": {"chartId": s6.id, "height": 50, "width": 12, "sliceName": s6.slice_name}}

    # Row 3 (s7): width 12 (Hiện diện và Nhập điểm - full width)
    position_json[f"CHART-{s7.id}"] = {"children": [], "id": f"CHART-{s7.id}", "type": "CHART", "meta": {"chartId": s7.id, "height": 55, "width": 12, "sliceName": s7.slice_name}}

    # Row 4 (s8, s9, s10): width 4 each
    for s in [s8, s9, s10]:
        position_json[f"CHART-{s.id}"] = {"children": [], "id": f"CHART-{s.id}", "type": "CHART", "meta": {"chartId": s.id, "height": 50, "width": 4, "sliceName": s.slice_name}}

    # Row 5 (s11): width 12
    position_json[f"CHART-{s11.id}"] = {"children": [], "id": f"CHART-{s11.id}", "type": "CHART", "meta": {"chartId": s11.id, "height": 60, "width": 12, "sliceName": s11.slice_name}}

    json_metadata = {
        "label_colors": {
            "Cần cải thiện": "#EF4444",
            "Cần theo dõi": "#F59E0B",
            "Đạt": "#3B82F6",
            "Tốt": "#10B981",
            "Xuất sắc": "#8B5CF6"
        },
        "native_filter_configuration": [
            {
                "id": "NATIVE_FILTER-ky_danh_gia",
                "name": "1. Chọn Năm / Kỳ đánh giá",
                "filterType": "filter_select",
                "targets": [{"datasetId": datasource_id, "column": {"name": "ky_danh_gia"}}],
                "defaultDataMask": {"filterState": {"value": None}},
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True, "searchAllOptions": True}
            },
            {
                "id": "NATIVE_FILTER-don_vi_dao_tao",
                "name": "2. Chọn Đơn vị đào tạo",
                "filterType": "filter_select",
                "targets": [{"datasetId": datasource_id, "column": {"name": "don_vi_dao_tao"}}],
                "defaultDataMask": {"filterState": {"value": None}},
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True, "searchAllOptions": True}
            }
        ]
    }

    dash = Dashboard(
        dashboard_title="TIẾN TRÌNH ĐÀO TẠO",
        slug="tien-trinh-dao-tao",
        published=True,
        position_json=json.dumps(position_json),
        json_metadata=json.dumps(json_metadata),
        slices=all_slices
    )
    if admin_user:
        dash.owners.append(admin_user)
    db.session.add(dash)
    db.session.commit()

    print(f"🎉 ĐÃ THIẾT LẬP THÀNH CÔNG DASHBOARD 'TIẾN TRÌNH ĐÀO TẠO' VỚI GIAO DIỆN GIÃN CỘT RỘNG RÃI (ID={dash.id})!")
