# 📊 Phân Tích Dự Án: University Data Lakehouse (CUSC)

## 1. Tổng Quan Dự Án

Đây là một đồ án triển khai kiến trúc **Data Lakehouse** cho hệ thống quản lý KPI giáo dục đại học (CUSC), áp dụng mô hình **Medallion Architecture** (Bronze → Silver → Gold), tích hợp nhiều công nghệ dữ liệu hiện đại.

**Mục tiêu**: Thu thập, xử lý, lưu trữ và trực quan hóa dữ liệu KPI chất lượng giáo dục từ các file PDF/DOCX của các đơn vị trong trường.

---

## 2. Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────────┐
│                        NGUỒN DỮ LIỆU                                │
│   [PDF/DOCX Files]  ──Upload──>  [Frontend (React)]                │
│   [Kafka Events]   ──Stream──>  [Spark Streaming]                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TẦNG STORAGE: MinIO (S3-compatible)              │
│  bucket: university-lakehouse                                       │
│  ├── bronze/unstructured_data/  (PDF/DOCX gốc)                     │
│  ├── bronze/structured_data/    (Parquet thô sau parse)             │
│  ├── bronze/cusc_kpi_stream/    (Parquet từ Kafka streaming)        │
│  └── iceberg-warehouse/         (Iceberg tables Silver + Gold)      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               PIPELINE XỬ LÝ: Apache Spark + PySpark               │
│                                                                     │
│  [BRONZE] spark_ingest_bronze.py                                    │
│     └─> Parse PDF/DOCX -> Parquet (boto3 + pdfplumber + docx)      │
│                               │                                     │
│  [SILVER] spark_bronze_to_silver.py                                 │
│     └─> Parquet -> Iceberg Table (kpi_cusc_master)                  │
│         + Nessie branch/merge + Data Quality Check                  │
│                               │                                     │
│  [GOLD] spark_silver_to_gold.py                                     │
│     └─> Silver -> 2 Data Marts (tổng hợp + chi tiết)               │
│         + Nessie branch/merge + Data Quality Check                  │
│                                                                     │
│  [STREAM] spark_streaming_bronze.py                                 │
│     └─> Kafka topic -> Parquet streaming (append mode)             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              CATALOG & VERSIONING: Nessie + Iceberg                 │
│  - Nessie: Git-like versioning cho Iceberg catalog                 │
│  - Iceberg: ACID table format trên MinIO                           │
│  - PostgreSQL (TimescaleDB): backend store cho Nessie               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      QUERY LAYER: Trino                             │
│  - Query Iceberg tables qua Nessie catalog                         │
│  - Superset kết nối qua sqlalchemy-trino                           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PRESENTATION LAYER                                │
│  ┌──────────────────┐      ┌──────────────────────────────────┐    │
│  │ Apache Superset  │      │ Custom Web App                    │    │
│  │ (Charts/Reports) │      │ FastAPI Backend + React Frontend  │    │
│  └──────────────────┘      └──────────────────────────────────┘    │
│                                                                     │
│  ┌──────────────────────────────────────────────┐                  │
│  │ OpenMetadata (Data Lineage & Catalog)         │                  │
│  │ - Lineage: Bronze -> Silver -> Gold           │                  │
│  └──────────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Phân Tích Từng Thành Phần

### 3.1 Infrastructure (docker-compose.yml)

| Service | Image | Port | Vai trò |
|---|---|---|---|
| `postgres` | timescale/timescaledb:latest-pg15 | 5432 | Backend store cho Nessie + App DB |
| `kafka` | apache/kafka:4.1.1 | 9094 | Message broker cho streaming |
| `minio` | minio/minio:latest | 9000/9001 | Object storage (S3-compatible) |
| `trino` | trinodb/trino:435 | 8081 | SQL query engine |
| `nessie` | ghcr.io/projectnessie/nessie:0.104.5 | 19120 | Catalog versioning |
| `superset` | apache/superset:3.1.0 | 8088 | BI/Visualization |

**✅ Nhận xét**: Bộ infrastructure đầy đủ, hợp lý cho một đồ án lakehouse. Tất cả trên một docker-compose là phù hợp với môi trường demo/học thuật.

---

### 3.2 Data Pipeline (Medallion Architecture)

#### 🥉 Tầng Bronze — `spark_ingest_bronze.py`
- **Input**: File PDF/DOCX từ MinIO (`bronze/unstructured_data/`)
- **Logic**: `pdfplumber` (parse bảng PDF) + `python-docx` (parse bảng Word)
- **Output**: Parquet (`bronze/structured_data/data_extracted.parquet`)
- **Schema**: `ma_chi_tieu`, `nhom_don_vi`, `quy_danh_gia`, `dinh_ky_thu_thap`, `muc_dang_ky`, `muc_dat`, `ket_qua_he_thong`, `checksum_sha256`

> [!NOTE]
> Bronze chạy bằng **boto3 thuần** (không qua Spark), phù hợp vì đây là tác vụ parse file, không cần Spark. Tuy nhiên tên file `spark_ingest_bronze.py` gây nhầm lẫn.

#### 🥈 Tầng Silver — `spark_bronze_to_silver.py`
- **Input**: Parquet Bronze từ MinIO
- **Logic**: `MERGE INTO` Iceberg (dedup bằng `checksum_sha256`)
- **Output**: Iceberg table `lakehouse.silver.kpi_cusc_master`
- **Nessie Flow**: `create_branch` → `use_branch` → MERGE → `check_quality_silver` → `merge_branch_to_main`

#### 🥇 Tầng Gold — `spark_silver_to_gold.py`
- **Input**: Silver Iceberg table
- **Logic**: Aggregate → 2 Data Marts
  - `kpi_tong_hop_don_vi`: tổng hợp tỷ lệ hoàn thành KPI theo đơn vị
  - `kpi_chi_tiet_dashboard`: chi tiết đầy đủ để tra cứu
- **Output**: 2 Iceberg tables `lakehouse.gold.*`

#### 🌊 Streaming — `spark_streaming_bronze.py`
- **Input**: Kafka topic `cusc_kpi_events`
- **Output**: Parquet streaming vào `bronze/cusc_kpi_stream/`

---

### 3.3 Catalog Versioning — `nessie_catalog_utils.py`

Đây là **điểm sáng về kỹ thuật** của dự án: áp dụng Git workflow cho dữ liệu.

```
main ──────────────────────────────────────── (stable)
        │                    ↑
        └─> feature_branch ──┘ (merge nếu pass QC)
              ↓
        [transform + data quality check]
              ↓ fail → không merge, branch còn nguyên để debug
```

**Các thao tác**: `create_branch`, `use_branch`, `merge_branch_to_main`, `drop_branch`, `create_tag`

**Data Quality Checks**:
- Silver: không NULL key, không trùng checksum, không rỗng
- Gold: không rỗng, tỷ lệ % ∈ [0, 100]

---

### 3.4 Metadata & Lineage — `openmetadata_lineage_utils.py`




- **Vấn đề thiết kế**: Lineage được đẩy thủ công trong pipeline vì Spark ghi qua Nessie, không qua Trino → OpenMetadata không auto-detect được lineage. Giải pháp thủ công này là hợp lý.

---

### 3.5 Backend API — FastAPI

| Endpoint | Chức năng |
|---|---|
| `POST /api/upload` | Upload file PDF/DOCX lên MinIO bronze |
| `GET /api/catalog/history` | Lấy commit history từ Nessie |
| `GET /api/catalog/references` | Lấy danh sách branch/tag từ Nessie |
| `POST /login` | JWT authentication |

> [!NOTE]
> `catalog.py` route bị **import nhưng không khai báo router trong main.py catalog imports** ở lần đầu — đã fix ở dòng 20-21. Route catalog chưa được bảo vệ authentication ở catalog.py (dòng 39 có `Depends(get_current_user)` → thực ra đã có).

---

### 3.6 Frontend — React + Vite + TailwindCSS

**Các trang**:
- `/login` — Đăng nhập JWT
- `/user` — Upload file (PDF/DOCX → MinIO)
- `/admin` — Admin Dashboard (embed Superset iframe + catalog history)

**Trang `CatalogHistoryTimeline.jsx`** tồn tại nhưng **chưa được route** trong `App.jsx`.

---

## 4. Đánh Giá Hướng Đi

### ✅ Những điểm TỐT

| Điểm mạnh | Chi tiết |
|---|---|
| **Medallion Architecture đúng chuẩn** | Bronze/Silver/Gold phân tầng rõ ràng, mỗi tầng có mục tiêu riêng |
| **Git-for-data với Nessie** | Đây là tính năng nổi bật nhất — branch/merge/quality-gate giống Git workflow |
| **Data Quality Gate** | Kiểm tra chất lượng trước khi merge vào main, rollback tự động nếu lỗi |
| **Dedup bằng checksum** | `MERGE INTO` với `checksum_sha256` đảm bảo idempotency |
| **Đa nguồn dữ liệu** | Batch (PDF/DOCX) + Streaming (Kafka) song song |
| **Lineage tracking** | OpenMetadata cho phép trace nguồn gốc dữ liệu Bronze → Silver → Gold |
| **Tech stack hiện đại** | Iceberg + Nessie + Trino + MinIO là bộ lakehouse chuẩn production |

### ⚠️ Những điểm CẦN CẢI THIỆN

#### 🔴 Vấn đề cao (Blocking)




#### 🟡 Vấn đề trung bình

4. **`CatalogHistoryTimeline.jsx` chưa được route**
   - Component đã code nhưng không có route trong `App.jsx`
   - → Cần thêm route `/catalog` hoặc tích hợp vào AdminDashboard


7. **Backend chưa trigger pipeline** airflow 
   - Sau khi upload file lên MinIO, không có cơ chế tự động kích hoạt pipeline Bronze → Silver → Gold
   - → Cần webhook/scheduler hoặc API endpoint để trigger Spark job

#### 🟢 Vấn đề nhỏ (Nice-to-have)

8. **Superset Dashboard hardcode URL và filter key**
   - `AdminDashboard.jsx` dòng 72: hardcode URL Superset với `native_filters_key`
   - → Nên đưa vào `.env`

9. **OpenMetadata chỉ là best-effort**
   - Lineage đẩy trong `try/except` và bị bỏ qua nếu lỗi
   - → Hành vi này OK cho MVP, nhưng về sau cần log/alert

---

## 5. Đề Xuất Luồng Dữ Liệu Hoàn Chỉnh

```
User Upload File (PDF/DOCX)
        │
        ▼
FastAPI /api/upload
        │
        ▼
MinIO: bronze/unstructured_data/
        │
        ▼  [Trigger: tự động sau upload hoặc cron]
spark_ingest_bronze.py (boto3 + pdfplumber/docx)
        │
        ▼
MinIO: bronze/structured_data/data_extracted.parquet
        │
        ▼
spark_bronze_to_silver.py (PySpark + Nessie branch)
        ├─> Data Quality Check
        ├─> [PASS] merge branch vào main
        └─> [FAIL] giữ branch, alert
        │
        ▼
Iceberg Silver: lakehouse.silver.kpi_cusc_master
        │
        ▼
spark_silver_to_gold.py (PySpark + Nessie branch)
        ├─> Aggregate → kpi_tong_hop_don_vi
        ├─> Select → kpi_chi_tiet_dashboard
        ├─> Data Quality Check
        └─> merge vào main
        │
        ▼
Iceberg Gold: lakehouse.gold.*
        │
        ▼
Trino (query engine)
        │
        ├─> Apache Superset (charts/dashboards)
        └─> Admin Dashboard (iframe Superset)
                             │
                             └─> OpenMetadata (lineage Bronze→Silver→Gold)
```

---

## 6. Kết Luận

**Hướng đi của dự án là HỢP LÝ và có tính học thuật cao.** Kiến trúc áp dụng đúng chuẩn Data Lakehouse hiện đại với:

- ✅ Medallion Architecture rõ ràng
- ✅ Apache Iceberg (ACID, schema evolution, time travel)
- ✅ Nessie (catalog versioning — điểm sáng sáng tạo)
- ✅ Data Quality Gate tích hợp vào pipeline
- ✅ Trino làm query engine thống nhất
- ✅ OpenMetadata cho governance

**Những gì cần làm ngay để dự án hoàn thiện**:

1. **Fix Python environment** — tạo venv, cài đủ dependencies vào đúng interpreter
2. **Kết nối Upload → Trigger Pipeline** — sau upload cần tự động kích hoạt Bronze script
3. **Tích hợp CatalogHistoryTimeline** — component đã code, cần route và hiển thị trên AdminDashboard
4. **Làm rõ luồng Streaming** — Kafka → Bronze Parquet → (tiếp theo là gì?)
5. **Chuyển config sang `.env`** — tập trung hóa cấu hình, tránh hardcode

