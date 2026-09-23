# -*- coding: utf-8 -*-
"""
build_superset_dashboard.py
------------------------------------------------------------
Script tự động tạo toàn bộ Dashboard "KẾT QUẢ HỌC TẬP" trên Superset
Dùng DỮ LIỆU THẬT 100% TỪ API TRINO (Bảng lakehouse.gold.kpi_api_learning_summary)
Chuẩn hóa 100% Viz Type & Metrics tương thích hoàn hảo với Superset UI.
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

    print("🔍 Tìm kiếm Dataset kpi_api_learning_summary...")
    dataset = db.session.query(SqlaTable).filter(SqlaTable.table_name == 'kpi_api_learning_summary').first()
    
    if not dataset:
        print("⚠️ Chưa có Dataset 'kpi_api_learning_summary' trong Superset DB. Đang kết nối...")
        database = db.session.query(Database).filter(Database.database_name.ilike('%trino%')).first()
        if not database:
            database = db.session.query(Database).first()
        
        if database:
            dataset = SqlaTable(
                table_name='kpi_api_learning_summary',
                schema='gold',
                database_id=database.id
            )
            db.session.add(dataset)
            db.session.commit()
            try:
                dataset.fetch_metadata()
            except Exception:
                pass
            print(f"✅ Đã tạo Dataset kpi_api_learning_summary (ID={dataset.id})")
        else:
            print("❌ Không tìm thấy Database Trino trong Superset.")
            sys.exit(1)
    else:
        print(f"✅ Đã tìm thấy Dataset 'kpi_api_learning_summary' (ID={dataset.id})")

    dataset.main_dttm_col = "thoi_gian_dong_goi_gold"
    for col in dataset.columns:
        if col.column_name in ["thoi_gian_dong_goi_gold", "ingested_at"]:
            col.is_dttm = True
    db.session.commit()

    datasource_id = dataset.id
    datasource_type = 'table'
    datasource_name = f"{dataset.id}__table"

    # Xóa các Slice cũ (nếu có)
    existing_slices = db.session.query(Slice).filter(Slice.slice_name.like('KQHT -%')).all()
    for s in existing_slices:
        db.session.delete(s)
    db.session.commit()

    def make_metric(col_name, agg="SUM", label=None):
        return {
            "expressionType": "SIMPLE",
            "column": {"column_name": col_name},
            "aggregate": agg,
            "label": label or f"{agg}({col_name})"
        }

    base_params = {
        "datasource": datasource_name,
        "granularity_sqla": "thoi_gian_dong_goi_gold",
        "time_range": "No filter",
    }

    # 1. KPI - Tổng SV
    p1 = {**base_params,
        "viz_type": "big_number_total",
        "metric": make_metric("tong_sv_theo_hoc", "SUM", "Tổng SV"),
        "subheader": "Tổng số sinh viên",
        "header_font_size": 0.35,
        "subheader_font_size": 0.15,
    }
    s1 = Slice(slice_name="KQHT - KPI Tổng SV", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p1))
    db.session.add(s1)

    # 2. KPI - GPA TB
    p2 = {**base_params,
        "viz_type": "big_number_total",
        "metric": make_metric("tb_gpa_trung_binh", "AVG", "GPA TB"),
        "subheader": "GPA TB",
        "y_axis_format": ".2f",
        "header_font_size": 0.35,
    }
    s2 = Slice(slice_name="KQHT - KPI GPA TB", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p2))
    db.session.add(s2)

    # 3. KPI - Qua HP
    p3 = {**base_params,
        "viz_type": "big_number_total",
        "metric": make_metric("tb_qua_hp_pct", "AVG", "Qua HP (%)"),
        "subheader": "Qua HP (%)",
        "y_axis_format": ".1f",
        "header_font_size": 0.35,
    }
    s3 = Slice(slice_name="KQHT - KPI Qua HP", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p3))
    db.session.add(s3)

    # 4. KPI - Đúng tiến độ
    p4 = {**base_params,
        "viz_type": "big_number_total",
        "metric": make_metric("dung_tien_do_pct", "AVG", "Đúng tiến độ (%)"),
        "subheader": "Đúng tiến độ (%)",
        "y_axis_format": ".1f",
        "header_font_size": 0.35,
    }
    s4 = Slice(slice_name="KQHT - KPI Đúng tiến độ", viz_type="big_number_total", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p4))
    db.session.add(s4)

    # 5. GPA theo chương trình
    p5 = {**base_params,
        "viz_type": "dist_bar",
        "groupby": ["chuong_trinh"],
        "columns": [],
        "metrics": [make_metric("tb_gpa_trung_binh", "AVG", "GPA TB")],
        "show_controls": False,
        "show_bar_value": True,
        "show_legend": False,
        "y_axis_bounds": [0, 4],
        "y_axis_format": ".2f",
        "label_colors": {
            "Sư phạm Toán học": "#3B82F6",
            "Công nghệ sinh học": "#10B981",
            "Nuôi trồng thủy sản": "#FBBF24",
            "Ngôn ngữ Anh": "#8B5CF6",
            "Công nghệ thực phẩm": "#06B6D4",
            "Quản trị kinh doanh": "#F97316",
            "Luật": "#EC4899",
            "Khoa học máy tính": "#6366F1",
            "Công nghệ kỹ thuật hóa học": "#14B8A6",
            "Kỹ thuật xây dựng": "#EAB308"
        }
    }
    s5 = Slice(slice_name="KQHT - GPA theo chương trình", viz_type="dist_bar", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p5))
    db.session.add(s5)

    # 6. Qua HP & Đúng tiến độ
    p6 = {**base_params,
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "chuong_trinh",
        "metrics": [
            make_metric("tb_qua_hp_pct", "AVG", "Qua HP (%)"),
            make_metric("dung_tien_do_pct", "AVG", "Đúng tiến độ (%)")
        ],
        "groupby": [],
        "bar_stacked": False,
        "color_scheme": "googleCategory20",
        "show_value": True,
        "y_axis_bounds": [0, 100],
    }
    s6 = Slice(slice_name="KQHT - Qua HP & Đúng tiến độ", viz_type="echarts_timeseries_bar", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p6))
    db.session.add(s6)

    # 7. Cảnh báo & Nguy cơ nghỉ
    p7 = {**base_params,
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "chuong_trinh",
        "metrics": [
            make_metric("canh_bao", "SUM", "Cảnh báo"),
            make_metric("tong_nguy_co_nghi_hoc", "SUM", "Nguy cơ nghỉ")
        ],
        "groupby": [],
        "bar_stacked": False,
        "color_scheme": "d3Category10",
        "show_value": True,
        "show_legend": True,
    }
    s7 = Slice(slice_name="KQHT - Cảnh báo & Nguy cơ nghỉ", viz_type="echarts_timeseries_bar", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p7))
    db.session.add(s7)

    # 8. Cơ cấu sinh viên
    p8 = {**base_params,
        "viz_type": "pie",
        "groupby": ["chuong_trinh"],
        "metric": make_metric("tong_sv_theo_hoc", "SUM", "Tổng SV"),
        "is_donut": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True,
    }
    s8 = Slice(slice_name="KQHT - Cơ cấu sinh viên", viz_type="pie", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p8))
    db.session.add(s8)

    # 9. Xu hướng
    p9 = {**base_params,
        "viz_type": "echarts_timeseries_line",
        "x_axis": "ky_danh_gia",
        "metrics": [
            make_metric("tb_gpa_trung_binh", "AVG", "GPA TB"),
            make_metric("tb_qua_hp_pct", "AVG", "Tỷ lệ Qua HP (%)")
        ],
        "groupby": [],
        "show_legend": True,
        "show_value": True,
        "marker_enabled": True,
        "marker_size": 6,
        "color_scheme": "googleCategory20",
    }
    s9 = Slice(slice_name="KQHT - Xu hướng qua các kỳ", viz_type="echarts_timeseries_line", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p9))
    db.session.add(s9)

    # 10. Bảng Chi tiết Kết quả
    p10 = {**base_params,
        "viz_type": "table",
        "groupby": ["chuong_trinh"],
        "metrics": [
            make_metric("tong_sv_theo_hoc", "SUM", "Tổng SV theo học"),
            make_metric("tb_qua_hp_pct", "AVG", "Tỷ lệ Qua HP (%)"),
            make_metric("tb_gpa_trung_binh", "AVG", "GPA Trung bình"),
            make_metric("dung_tien_do_pct", "AVG", "Đúng tiến độ (%)"),
            make_metric("tong_nguy_co_nghi_hoc", "SUM", "Nguy cơ nghỉ"),
            make_metric("tong_can_bao_hoc_vu", "SUM", "Cảnh báo học vụ")
        ],
        "page_length": 10,
        "include_search": True,
        "query_mode": "aggregate"
    }
    s100_table = Slice(slice_name="KQHT - Chi tiết kết quả", viz_type="table", datasource_id=datasource_id, datasource_type=datasource_type, params=json.dumps(p10))
    db.session.add(s100_table)

    db.session.commit()

    all_slices = [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10]

    # Gán admin user làm Owner
    from flask_appbuilder.security.sqla.models import User
    admin_user = db.session.query(User).filter(User.username == 'admin').first()
    for s in all_slices:
        if admin_user and admin_user not in s.owners:
            s.owners.append(admin_user)
    db.session.commit()

    # Xóa Dashboard cũ nếu có
    existing_dash = db.session.query(Dashboard).filter(Dashboard.dashboard_title == 'KẾT QUẢ HỌC TẬP').first()
    if existing_dash:
        db.session.delete(existing_dash)
        db.session.commit()

    position_json = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "GRID_ID": {"children": ["ROW-1", "ROW-2", "ROW-3", "ROW-4"], "id": "GRID_ID", "type": "GRID"},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "KẾT QUẢ HỌC TẬP"}},
        "ROW-1": {
            "children": [f"CHART-{s1.id}", f"CHART-{s2.id}", f"CHART-{s3.id}", f"CHART-{s4.id}"],
            "id": "ROW-1",
            "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "ROW-2": {
            "children": [f"CHART-{s5.id}", f"CHART-{s6.id}"],
            "id": "ROW-2",
            "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "ROW-3": {
            "children": [f"CHART-{s7.id}", f"CHART-{s8.id}", f"CHART-{s9.id}"],
            "id": "ROW-3",
            "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        },
        "ROW-4": {
            "children": [f"CHART-{s10.id}"],
            "id": "ROW-4",
            "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"}
        }
    }

    for s in [s1, s2, s3, s4]:
        position_json[f"CHART-{s.id}"] = {
            "children": [], "id": f"CHART-{s.id}", "type": "CHART",
            "meta": {"chartId": s.id, "height": 26, "width": 3, "sliceName": s.slice_name}
        }

    for s in [s5, s6]:
        position_json[f"CHART-{s.id}"] = {
            "children": [], "id": f"CHART-{s.id}", "type": "CHART",
            "meta": {"chartId": s.id, "height": 50, "width": 6, "sliceName": s.slice_name}
        }

    for s in [s7, s8, s9]:
        position_json[f"CHART-{s.id}"] = {
            "children": [], "id": f"CHART-{s.id}", "type": "CHART",
            "meta": {"chartId": s.id, "height": 50, "width": 4, "sliceName": s.slice_name}
        }

    position_json[f"CHART-{s10.id}"] = {
        "children": [], "id": f"CHART-{s10.id}", "type": "CHART",
        "meta": {"chartId": s10.id, "height": 60, "width": 12, "sliceName": s10.slice_name}
    }

    json_metadata = {
        "label_colors": {
            "Sư phạm Toán học": "#3B82F6",
            "Công nghệ sinh học": "#10B981",
            "Nuôi trồng thủy sản": "#FBBF24",
            "Ngôn ngữ Anh": "#8B5CF6",
            "Công nghệ thực phẩm": "#06B6D4",
            "Quản trị kinh doanh": "#F97316",
            "Luật": "#EC4899",
            "Khoa học máy tính": "#6366F1",
            "Công nghệ kỹ thuật hóa học": "#14B8A6",
            "Kỹ thuật xây dựng": "#EAB308"
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
                "controlValues": {
                    "enableEmptyFilter": False,
                    "defaultToFirstItem": False,
                    "multiSelect": True,
                    "searchAllOptions": True
                }
            },
            {
                "id": "NATIVE_FILTER-chuong_trinh",
                "name": "2. Chọn Ngành / Chương trình",
                "filterType": "filter_select",
                "targets": [{"datasetId": datasource_id, "column": {"name": "chuong_trinh"}}],
                "defaultDataMask": {"filterState": {"value": None}},
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "type": "NATIVE_FILTER",
                "controlValues": {
                    "enableEmptyFilter": False,
                    "defaultToFirstItem": False,
                    "multiSelect": True,
                    "searchAllOptions": True
                }
            }
        ]
    }

    dash = Dashboard(
        dashboard_title="KẾT QUẢ HỌC TẬP",
        slug="ket-qua-hoc-tap",
        published=True,
        position_json=json.dumps(position_json),
        json_metadata=json.dumps(json_metadata),
        slices=all_slices
    )
    if admin_user:
        dash.owners.append(admin_user)
    db.session.add(dash)
    db.session.commit()

    print(f"🎉 ĐÃ THIẾT LẬP THÀNH CÔNG DASHBOARD 'KẾT QUẢ HỌC TẬP' (ID={dash.id}) VỚI DỮ LIỆU API THẬT!")
    print(f"👉 URL: http://localhost:8088/superset/dashboard/{dash.id}/?standalone=3")
