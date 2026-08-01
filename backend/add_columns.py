import sys
import os

# Ensure backend root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.database import engine
from sqlalchemy import text

def add_columns():
    print("==================================================")
    print("🚀 ĐANG THÊM CỘT dag_run_id VÀ pipeline_status VÀO upload_history")
    print("==================================================")
    
    try:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE upload_history ADD COLUMN dag_run_id VARCHAR(255);"))
status VARCHAR(50) DEFAULT 'pending';"))            conn.execute(text("ALTER TABLE upload_history ADD COLUMN pipeline_
            conn.commit()
            print("🎉 [THÀNH CÔNG] Đã thêm các cột vào bảng upload_history!")
    except Exception as e:
        print("\n❌ LỖI:")
        print(f"   {e}")
        # Lỗi thường là do cột đã tồn tại

if __name__ == "__main__":
    add_columns()
