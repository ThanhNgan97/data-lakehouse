import sys
import os
import logging

# Ensure backend root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.database import engine, Base, SessionLocal, get_password_hash
from db.models import User

def migrate_and_seed():
    print("==================================================")
    print("🚀 ĐANG KHỞI TẠO VÀ TẠO BẢNG DATABASE LAKEOUSE")
    print("==================================================")
    print(f"🔗 Database URL: {engine.url}")
    
    try:
        # Step 1: Create all tables defined in SQLAlchemy models (e.g. users table)
        print("\n1. Đang tạo cấu trúc bảng (Tables Schema)...")
        Base.metadata.create_all(bind=engine)
        print("   -> [THÀNH CÔNG] Bảng 'users' đã được tạo trong Cơ sở dữ liệu!")

        # Step 2: Seed initial admin user if not exists
        print("\n2. Đang kiểm tra và khởi tạo tài khoản Admin ban đầu...")
        db = SessionLocal()
        try:
            admin_user = db.query(User).filter(User.username == "admin").first()
            if not admin_user:
                admin_user = User(
                    username="admin",
                    email="admin@lakehouse.vn",
                    full_name="System Administrator",
                    hashed_password=get_password_hash("admin123"),
                    role="admin",
                    is_active=True
                )
                db.add(admin_user)
                
                user_user = User(
                    username="canbo_truongA",
                    email="canbo_truonga@lakehouse.vn",
                    full_name="Cán bộ Trường A",
                    hashed_password=get_password_hash("user123"),
                    role="user",
                    is_active=True
                )
                db.add(user_user)
                db.commit()
                print("   -> [THÀNH CÔNG] Đã tạo 2 tài khoản mặc định:")
                print("      + Admin: admin / admin123")
                print("      + User : canbo_truongA / user123")
            else:
                print(f"   -> Tài khoản 'admin' đã tồn tại trong DB.")
        finally:
            db.close()

        print("\n==================================================")
        print("🎉 TẠO BẢNG & MIGRATE THÀNH CÔNG!")
        print("==================================================")

    except Exception as e:
        print("\n❌ LỖI KẾT NỐI DATABASE:")
        print(f"   {e}")
        print("\n👉 HƯỚNG DẪN KHẮC PHỤC:")
        print("   1. Đảm bảo PostgreSQL hoặc Docker Postgres đang chạy.")
        print("   2. Kiểm tra thông tin PG_HOST, PG_PORT, PG_USER, PG_PASSWORD trong file .env")

if __name__ == "__main__":
    migrate_and_seed()
