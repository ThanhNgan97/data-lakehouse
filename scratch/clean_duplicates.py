# -*- coding: utf-8 -*-
import sqlalchemy

engine = sqlalchemy.create_engine('trino://admin@trino:8080/lakehouse')

print("🧹 Dọn dẹp bản ghi thử nghiệm trùng lặp ở tầng Silver...")
try:
    engine.execute("DELETE FROM silver.api_teaching_master WHERE don_vi_dao_tao = 'Trường Bách Khoa' OR don_vi_dao_tao = 'Trường Bách khoa 1'")
    print("✅ Đã xóa bản ghi thừa ở tầng Silver!")
except Exception as e:
    print(f"⚠️ Warning silver clean: {e}")

try:
    engine.execute("DELETE FROM gold.kpi_api_teaching_summary WHERE don_vi_dao_tao = 'Trường Bách Khoa' OR don_vi_dao_tao = 'Trường Bách khoa 1'")
    print("✅ Đã xóa bản ghi thừa ở tầng Gold!")
except Exception as e:
    print(f"⚠️ Warning gold clean: {e}")

res = engine.execute("SELECT don_vi_dao_tao FROM gold.kpi_api_teaching_summary").fetchall()
print("📊 Danh sách đơn vị chính thức sau khi dọn dẹp:", [r[0] for r in res])
