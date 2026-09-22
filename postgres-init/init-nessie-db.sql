-- Tạo database cho Nessie Catalog
CREATE DATABASE nessie_db;

-- Tạo database cho Superset (nếu sau này bạn muốn tách DB của Superset)
CREATE DATABASE superset_db;

-- Cấp quyền truy cập cho user postgres
GRANT ALL PRIVILEGES ON DATABASE nessie_db TO postgres;