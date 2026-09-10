import os
import logging
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from core.security import get_password_hash

# Setup Base for ORM models
Base = declarative_base()

# Read DB connection from environment; fallback to provided credentials
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "240203")
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "university_db")

# Build SQLAlchemy-compatible URL
_pw = quote_plus(PG_PASSWORD)
DATABASE_URL = f"postgresql://{PG_USER}:{_pw}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Create engine and session factory
# Do not let an unavailable PostgreSQL instance leave API requests hanging until
# the operating-system TCP timeout expires.
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 5},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# NOTE: In-memory fallback removed. Authentication now requires a real DB.

def get_db():
    """Yield a SQLAlchemy session for dependency injection."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Tự động tạo các bảng và seed tài khoản mặc định vào database."""
    try:
        from db.models import User, UploadHistory, DataConnector
        Base.metadata.create_all(bind=engine)
    

        db = SessionLocal()
        try:
            existing_user = db.query(User).first()
            if not existing_user:
                logging.info("Initializing database with default users...")
                admin_user = User(
                    username="admin",
                    email="admin@lakehouse.vn",
                    full_name="System Administrator",
                    hashed_password=get_password_hash("admin123"),
                    role="admin",
                    is_active=True
                )
                normal_user = User(
                    username="canbo_truongA",
                    email="canbo_truonga@lakehouse.vn",
                    full_name="Cán bộ Trường A",
                    hashed_password=get_password_hash("user123"),
                    role="user",
                    is_active=True
                )
                db.add(admin_user)
                db.add(normal_user)
                db.commit()
                logging.info("Default users (admin, canbo_truongA) created successfully.")

            # -------------------------------------------------
            # Seed Default MySQL Connector
            # -------------------------------------------------
            from core.connector_crypto import encrypt_secret

            default_connector = (
                db.query(DataConnector)
                .filter(DataConnector.is_default.is_(True))
                .first()
            )

            if not default_connector:
                mysql_password = os.getenv("MYSQL_PASSWORD")

                if not mysql_password:
                    logging.warning(
                        "MYSQL_PASSWORD is not configured. "
                        "Default MySQL connector was not seeded."
                    )
                else:
                    default_connector = DataConnector(
                        name="CUSC KPI Operational",
                        connector_type="MYSQL",
                        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
                        port=int(os.getenv("MYSQL_PORT", "3306")),
                        database_name=os.getenv(
                            "MYSQL_DATABASE",
                            "cusc_kpi_operational",
                        ),
                        username=os.getenv(
                            "MYSQL_USER",
                            "kpi_user",
                        ),
                        password_encrypted=encrypt_secret(mysql_password),
                        source_config={
                            "mode": "cusc_kpi_operational",
                            "tables": [
                                "don_vi",
                                "muc_tieu_kpi",
                                "ket_qua_danh_gia",
                            ],
                            "primary_table": "ket_qua_danh_gia",
                        },
                        schema_mapping=None,
                        is_default=True,
                        is_active=True,
                        last_test_status="not_tested",
                    )

                    db.add(default_connector)
                    db.commit()
                    logging.info(
                        "Default MySQL connector created successfully."
                    )
        finally:
            db.close()
    except Exception as e:
        logging.warning(f"Database initialization warning (PostgreSQL offline or connecting issue): {e}")

def get_user(username: str, db):
    """Lấy thông tin user từ database.

    `db` must be a SQLAlchemy `Session` provided by `get_db()`.
    Returns a `User` ORM instance or `None`.
    """
    from db.models import User
    return db.query(User).filter(User.username == username).first()
