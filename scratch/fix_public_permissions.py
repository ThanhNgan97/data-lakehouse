# -*- coding: utf-8 -*-
"""
fix_public_permissions.py
------------------------------------------------------------
1. Cấp đầy đủ quyền cho Role 'Public' & 'Gamma' trong Superset Security Manager
   để người dùng chưa đăng nhập (Guest) hoặc vừa vào web vẫn xem được đầy đủ
   Danh sách Dashboard, Charts, Datasets và dữ liệu biểu đồ.
2. Kiểm tra và liên kết chắc chắn 11 Slices vào Dashboard 2 ('TIẾN TRÌNH ĐÀO TẠO') & Dashboard 1 ('KẾT QUẢ HỌC TẬP').
------------------------------------------------------------
"""
import sys

from superset.app import create_app
app = create_app()

with app.app_context():
    from superset import db, security_manager
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable

    print("🔧 1. Tiến hành cấp quyền Public & Gamma...")
    public_role = security_manager.find_role("Public")
    gamma_role = security_manager.find_role("Gamma")

    p_all_ds = security_manager.find_permission_view_menu("all_datasource_access", "all_datasource_access")
    p_all_db = security_manager.find_permission_view_menu("all_database_access", "all_database_access")

    for role in [public_role, gamma_role]:
        if role:
            if p_all_ds and p_all_ds not in role.permissions:
                role.permissions.append(p_all_ds)
            if p_all_db and p_all_db not in role.permissions:
                role.permissions.append(p_all_db)
            
            # Cấp thêm quyền đọc Dashboard, Chart, Dataset
            for perm_name, view_name in [
                ("can_read", "Dashboard"),
                ("can_read", "Chart"),
                ("can_read", "Dataset"),
                ("can_read", "Superset"),
                ("can_get", "DashboardRestApi"),
                ("can_get", "ChartRestApi"),
                ("can_get", "DatasetRestApi"),
                ("can_list", "DashboardRestApi"),
                ("can_list", "ChartRestApi"),
                ("can_list", "DatasetRestApi"),
            ]:
                pv = security_manager.find_permission_view_menu(perm_name, view_name)
                if pv and pv not in role.permissions:
                    role.permissions.append(pv)

    db.session.commit()
    print("✅ Đã cấp quyền Public & Gamma thành công!")

    print("\n🔍 2. Kiểm tra Dashboard 'TIẾN TRÌNH ĐÀO TẠO' (ID=2)...")
    d2 = db.session.query(Dashboard).filter(Dashboard.id == 2).first()
    if not d2:
        d2 = db.session.query(Dashboard).filter(Dashboard.slug == 'tien-trinh-dao-tao').first()

    if d2:
        print(f"✅ Đã tìm thấy Dashboard 'TIẾN TRÌNH ĐÀO TẠO' (ID={d2.id})")
        d2.published = True
        
        # Lấy tất cả Slices có tiền tố 'TP -'
        tp_slices = db.session.query(Slice).filter(Slice.slice_name.like('TP - %')).all()
        print(f"📊 Tìm thấy {len(tp_slices)} biểu đồ cho TIẾN TRÌNH ĐÀO TẠO:")
        for s in tp_slices:
            print(f"   - Slice ID {s.id}: {s.slice_name} ({s.viz_type})")
            if s not in d2.slices:
                d2.slices.append(s)

        # Gán Admin làm owner
        admin_user = security_manager.find_user('admin')
        if admin_user and admin_user not in d2.owners:
            d2.owners.append(admin_user)

        db.session.commit()
        print(f"🎉 Dashboard {d2.id} hiện có {len(d2.slices)} biểu đồ liên kết!")
    else:
        print("❌ Không tìm thấy Dashboard 'TIẾN TRÌNH ĐÀO TẠO'")

    print("\n🔍 3. Kiểm tra Dashboard 'KẾT QUẢ HỌC TẬP' (ID=1)...")
    d1 = db.session.query(Dashboard).filter(Dashboard.id == 1).first()
    if d1:
        d1.published = True
        kq_slices = db.session.query(Slice).filter(Slice.slice_name.like('KQHT - %')).all()
        for s in kq_slices:
            if s not in d1.slices:
                d1.slices.append(s)
        db.session.commit()
        print(f"🎉 Dashboard {d1.id} hiện có {len(d1.slices)} biểu đồ liên kết!")

    print("\n🚀 HOÀN TẤT ĐỒNG BỘ QUYỀN VÀ BIỂU ĐỒ TRÊN SUPERSET!")
