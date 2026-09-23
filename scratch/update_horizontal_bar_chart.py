# -*- coding: utf-8 -*-
"""
update_horizontal_bar_chart.py
------------------------------------------------------------
Chuyển đổi Chart "TP - Tỷ lệ đúng tiến độ" từ biểu đồ cột đứng (Hình 1)
sang biểu đồ thanh nằm ngang (Horizontal Bar Chart) có màu sắc sinh động
giống 100% thiết kế ở Hình 2.
------------------------------------------------------------
"""
import json
from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard

    s = db.session.query(Slice).filter(Slice.slice_name == 'TP - Tỷ lệ đúng tiến độ').first()
    if s:
        print(f"🔍 Đang cập nhật Slice '{s.slice_name}' (ID={s.id})...")
        
        # Cấu hình mới cho ECharts Bar Chart ở dạng nằm ngang (horizontal)
        new_params = {
            "datasource": s.datasource_id_str,
            "granularity_sqla": "thoi_gian_dong_goi_gold",
            "time_range": "No filter",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "don_vi_dao_tao",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "tb_dung_tien_do_pct"},
                    "aggregate": "AVG",
                    "label": "Tỷ lệ đúng tiến độ (%)"
                }
            ],
            "groupby": [],
            "orientation": "horizontal",  # 🌟 CHUYỂN THÀNH NẰM NGANG GIỐNG HÌNH 2!
            "bar_stacked": False,
            "show_value": True,
            "show_legend": False,
            "y_axis_bounds": [0, 100],
            "y_axis_format": ".1f",
            "color_scheme": "googleCategory20",
            # Phối màu tùy chỉnh rực rỡ cho từng đơn vị đào tạo
            "label_colors": {
                "Trường Bách khoa": "#3B82F6",
                "Trường Bách Khoa": "#3B82F6",
                "Trường Khoa học Tự nhiên": "#10B981",
                "Trường Kinh tế": "#8B5CF6",
                "Khoa Ngoại ngữ": "#F59E0B",
                "Trường Sư phạm": "#06B6D4",
                "Trường CNTT&TT": "#6366F1",
                "Trường Nông nghiệp": "#EC4899",
                "Trường Thủy sản": "#14B8A6",
                "Trường KHXH&NV": "#EAB308",
                "Viện CNSH&TP": "#84CC16"
            }
        }

        s.viz_type = "echarts_timeseries_bar"
        s.params = json.dumps(new_params)
        db.session.commit()
        print("✅ Đã cập nhật xong Chart 'TP - Tỷ lệ đúng tiến độ' sang dạng thanh nằm ngang!")

        # Đồng bộ lại Dashboard TIẾN TRÌNH ĐÀO TẠO
        d2 = db.session.query(Dashboard).filter(Dashboard.id == 2).first()
        if d2:
            if s not in d2.slices:
                d2.slices.append(s)
            db.session.commit()
            print(f"✅ Đã đồng bộ lên Dashboard ID={d2.id}!")
    else:
        print("❌ Không tìm thấy Slice 'TP - Tỷ lệ đúng tiến độ'")
