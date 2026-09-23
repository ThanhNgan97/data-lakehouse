# -*- coding: utf-8 -*-
import sqlalchemy

engine = sqlalchemy.create_engine('trino://admin@trino:8080/lakehouse')

q = """
SELECT 
    don_vi_dao_tao, 
    diem_phan_hoi_sv, 
    AVG(TRY_CAST(REPLACE(SPLIT_PART(diem_phan_hoi_sv, '/', 1), ',', '.') AS DOUBLE)) as diem_num
FROM gold.kpi_api_teaching_summary
GROUP BY don_vi_dao_tao, diem_phan_hoi_sv
ORDER BY diem_num DESC
"""

res = engine.execute(q).fetchall()
print("📊 Kết quả chuyển đổi toàn bộ 10 đơn vị sang số thập phân (dấu chấm):")
for r in res:
    print(f"  • {r[0]}: {r[1]} => {r[2]}")
