# 📋 KẾ HOẠCH NÂNG CẤP AIRFLOW: AI-AUGMENTED UNIVERSAL DATA LAKEHOUSE

> **Mục tiêu cốt lõi**: Chuyển đổi hệ thống từ pipeline xử lý KPI cố định sang một **Data Lakehouse thông minh, đa nguồn**, có khả năng tiếp nhận mọi loại dữ liệu (File upload UI, JSON API, Database khách hàng), dùng **AI (Gemini) làm Semantic Router** để phân loại, suy luận schema và định tuyến dữ liệu vào nhánh xử lý tự động của Apache Iceberg, sẵn sàng hiển thị trên Trino và Superset.

---

## 📌 BẢNG THEO DÕI TIẾN ĐỘ TỔNG THỂ

| Giai đoạn | Nội dung công việc | Trạng thái | Ngày hoàn thành | Ghi chú |
|---|---|:---:|:---:|---|
| **Phase 0** | Khảo sát hệ thống, lập kế hoạch & chuẩn hóa cơ chế ghi log lỗi | ✅ COMPLETED | 2026-10-06 | Đã lập và phê duyệt kế hoạch |
| **Phase 1** | Module AI Semantic Profiler & Schema Inferer (`ai_dataset_router.py`) | ✅ COMPLETED | 2026-10-06 | Đã kiểm thử thành công trên cả Host và Docker Container Airflow (Gemini + Fallback) |
| **Phase 2** | Generic Dynamic Silver & Gold Processor (PySpark + Iceberg) | ✅ COMPLETED | 2026-10-06 | Đã test thành công nạp dữ liệu động, sinh bảng Iceberg Silver & Gold, Nessie branch merge |
| **Phase 3** | Xây dựng DAG Airflow đa năng (`universal_lakehouse_pipeline.py`) | ✅ COMPLETED | 2026-10-06 | Đã tạo DAG tích hợp AI Profiler, BranchPythonOperator và TaskGroups |
| **Phase 4** | Auto-Registration Trino & Cấu hình Visualize Superset | ✅ COMPLETED | 2026-10-06 | Đã mở rộng API preview Trino động và tạo module tự sinh metadata Superset dataset |
| **Phase 5** | Kiểm thử End-to-End (E2E), tối ưu hiệu năng & nghiệm thu | ✅ COMPLETED | 2026-10-06 | 5/5 E2E Tests Pass (KPI legacy, Registered API, New Dataset, Superset YAML, Security) |
| **Phase 6** | Catalog-Aware Semantic Router & Column Mapping (Chống trùng lặp bảng hao hao nhau) | ✅ COMPLETED | 2026-10-06 | Tự động lấy danh sách bảng Silver từ Nessie, so khớp thực thể và tự sinh column mapping (Test Case 6 Pass) |

---

## 🏗️ THIẾT KẾ KIẾN TRÚC HỆ THỐNG

### 1. Nguyên tắc phân tách: Control Plane (AI) vs Data Plane (Spark)

```
                            NGUỒN DỮ LIỆU ĐA DẠNG
         (UI Upload Excel/CSV/JSON/PDF, DB Dump, REST API Payload)
                                     │
                                     ▼
        ┌─────────────────────────────────────────────────────────┐
        │        TẦNG 1: BRONZE LANDING ZONE (MinIO Storage)       │
        │      Lưu nguyên trạng dữ liệu: bronze/landing/<batch_id> │
        └────────────────────────────┬────────────────────────────┘
                                     │
                                     ▼
        ┌─────────────────────────────────────────────────────────┐
        │   TẦNG 2: AI SEMANTIC PROFILER & ROUTER (Control Plane)  │
        │   - Lấy mẫu (sample 5-10 dòng đầu)                     │
        │   - Gemini phân tích ngữ nghĩa, định danh Domain        │
        │   - Suy luận: Business Key, Timestamp, Metrics, Dims   │
        │   - Output: Routing Decision Metadata (JSON chuẩn)     │
        └────────────────────────────┬────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │ Quyết định định tuyến của AI?   │
                    └────────────────┬────────────────┘
                                     │
            ┌────────────────────────┼────────────────────────┐
            ▼                        ▼                        ▼
    [Nhánh 1: KPI Legacy]    [Nhánh 2: CTU API]      [Nhánh 3: Generic Dynamic]
    (File Word/PDF KPI)      (Học tập/Giảng dạy)     (Dữ liệu mới bất kỳ)
            │                        │                        │
            └────────────────────────┼────────────────────────┘
                                     │
                                     ▼
        ┌─────────────────────────────────────────────────────────┐
        │        TẦNG 3: APACHE SPARK & ICEBERG (Data Plane)      │
        │   - Phân loại & deduplicate (generic_silver_classifier) │
        │   - Apache Iceberg Schema Evolution (tự sinh cột mới)  │
        │   - Quarantine các bản ghi sai lệch                    │
        │   - Nessie Branching (Git-like Data Versioning)         │
        └────────────────────────────┬────────────────────────────┘
                                     │
                                     ▼
        ┌─────────────────────────────────────────────────────────┐
        │          TẦNG 4: QUERY & VISUALIZATION LAYER            │
        │   - Trino Catalog tự động nhận diện bảng mới từ Nessie  │
        │   - Superset query trực tiếp hoặc tự động import dataset │
        └─────────────────────────────────────────────────────────┘
```

---

## 📑 CHI TIẾT CÁC GIAI ĐOẠN TRIỂN KHAI

### Phase 1: Module AI Semantic Profiler & Schema Inferer
- **Mục tiêu**: Xây dựng module Python độc lập, nhận vào đường dẫn dữ liệu Bronze (sample), trả về metadata điều hướng chuẩn Pydantic.
- **File tạo mới**: `lakehouse/spark/ai_dataset_router.py`
- **Output Schema của AI**:
  ```python
  class RoutingDecision(BaseModel):
      dataset_domain: str          # VD: education, finance, hr, kpi, generic
      dataset_entity: str          # VD: student_scores, course_fees
      route_target: str            # 'legacy_kpi' | 'registered_api' | 'generic_dynamic'
      target_silver_table: str     # VD: lakehouse.silver.student_scores
      target_quarantine_table: str # VD: lakehouse.silver.student_scores_quarantine
      business_keys: list[str]     # Danh sách cột làm khóa chính tự nhiên
      source_updated_at_field: str # Cột mốc thời gian cập nhật (hoặc auto-generated)
      metric_columns: list[str]    # Cột định lượng (để aggregate lên Gold)
      dimension_columns: list[str] # Cột phân loại (để group-by)
      fallback_used: bool          # True nếu AI không chắc và phải dùng surrogate key
  ```
- **Xử lý ngoại lệ**:
  - Nếu Gemini API lỗi (hết quota / mất mạng): Tự động fallback về **Deterministic Schema Inferer** (dùng PySpark tự động suy luận schema + băm toàn bộ dòng làm `surrogate_key`).

### Phase 2: Generic Dynamic Silver & Gold Processor
- **Mục tiêu**: Xây dựng bộ máy nạp dữ liệu động vào Iceberg, không bị phụ thuộc vào bất kỳ bảng cố định nào.
- **File tạo mới / cập nhật**: `lakehouse/spark/spark_generic_dynamic_processor.py`
- **Kế thừa các module sẵn có**:
  - `generic_silver_classifier.py`
  - `generic_silver_merge.py`
  - `generic_silver_quarantine.py`
  - `generic_silver_dedup.py`
- **Tính năng Iceberg Schema Evolution**:
  - Tự động tạo bảng nếu chưa có: `CREATE TABLE IF NOT EXISTS lakehouse.silver.<entity> ...`
  - Tự động bổ sung cột nếu nguồn gửi thêm cột mới: Cấu hình `spark.sql.iceberg.schema-evolution.enabled = true` hoặc kiểm tra lệch schema để `ALTER TABLE ADD COLUMNS`.

### Phase 3: Dynamic Airflow DAG (`universal_lakehouse_pipeline.py`)
- **Mục tiêu**: DAG duy nhất quản lý toàn bộ vòng đời dữ liệu từ lúc nạp đến lúc hiển thị.
- **File tạo mới**: `lakehouse/dags/universal_lakehouse_pipeline.py`
- **Các Tasks chính**:
  1. `stage_and_sample`: Nhận diện file/payload mới trong MinIO `staging/` hoặc `bronze/landing/`.
  2. `ai_semantic_profiling`: Chạy `ai_dataset_router.py` để quyết định `route_target`.
  3. `branch_decision`: `BranchPythonOperator` chia nhánh:
     - Nhánh A (`route_legacy_kpi`): Kích hoạt các script KPI cũ.
     - Nhánh B (`route_registered_api`): Kích hoạt `api_dataset_orchestration.py`.
     - Nhánh C (`route_generic_dynamic`): Chạy `spark_generic_dynamic_processor.py`.
  4. `process_silver`: Ghi dữ liệu vào Iceberg Silver.
  5. `process_gold_auto`: Tự động aggregate các cột metrics theo dimensions.
  6. `trino_register_and_smoke_test`: Kiểm tra Trino truy vấn được bảng mới.

### Phase 4: Auto-Registration Trino & Visualize Superset
- **Mục tiêu**: Ngay sau khi pipeline chạy xong, dữ liệu có thể xem được ngay trên Trino và Superset.
- **Trino**: Tự động nhận diện bảng qua Nessie catalog (Zero-config).
- **Superset**:
  - Cung cấp script tự động tạo YAML dataset và import vào Superset qua CLI.
  - Cập nhật Backend FastAPI `/api/pipeline/` để bổ sung endpoint cho phép UI người dùng khám phá (Data Catalog Discovery) các bảng mới sinh.

### Phase 5: Testing, Benchmarking & Hoàn thiện
- Test Case 1: File Word/PDF KPI cũ (đảm bảo tính tương thích ngược 100%).
- Test Case 2: API `learning_outcomes` / `teaching_progress` (đảm bảo các API đã đăng ký chạy đúng nhánh).
- Test Case 3: Nạp 1 file CSV / JSON bất kỳ (ví dụ: Danh sách đóng tiền học phí, Điểm thi tuyển sinh) → Kiểm tra AI phân loại và tạo bảng Silver/Gold tự động.
- ✅ Đã test thành công với bộ kiểm thử E2E: `lakehouse/spark/tests/test_universal_ai_pipeline_e2e.py` (vượt qua 6/6 test cases).

### Phase 6: Catalog-Aware Matching & Column Mapping (Schema Drift)
- ✅ Cập nhật `ai_dataset_router.py`: Bổ sung hàm `fetch_existing_silver_tables()` để AI Router đối chiếu với danh mục bảng Silver thực tế trên Nessie/Iceberg.
- ✅ Cập nhật `spark_generic_dynamic_processor.py`: Đổi tên cột tự động theo `column_mapping` do AI suy luận khi đối chiếu schema cũ vs mới (giải quyết triệt để vấn đề 2025 vs 2026 lệch tên cột/thêm cột nhưng vẫn gom về 1 bảng).

### Phase 7: MinIO Staging ➔ Archive Ingestion & Backend Trigger Integration
- ✅ Cập nhật `universal_lakehouse_pipeline.py`:
  - Tự động quét file mới nhất trong MinIO prefix `staging/` khi trigger không truyền tham số.
  - Sau khi nạp vào Iceberg và smoke test Trino thành công (`join_and_smoke_test`), tự động di chuyển file từ `staging/` sang `archive/` (Copy + Delete) để dọn sạch hộp thư đến và lưu vết kiểm toán.
- ✅ Cập nhật `backend/api/routes/upload.py`:
  - Khi người dùng upload file qua Web UI/API, tự động kích hoạt `universal_lakehouse_pipeline` kèm payload cấu hình `conf`.
  - Endpoint kiểm tra trạng thái (`/upload/pipeline-status/{dag_run_id}`) hỗ trợ cả `universal_lakehouse_pipeline` và `lakehouse_pipeline` cũ.

---

## 🛑 BẢNG NHẬT KÝ LỖI & PHÒNG NGỪA (ERROR LOG & LESSONS LEARNED)

> **Quy tắc bắt buộc**: Mỗi khi gặp lỗi kỹ thuật trong quá trình thực hiện, BẮT BUỘC ghi lại vào bảng này để tránh lặp lại.

| STT | Thời gian | Lỗi gặp phải (Error Description) | Nguyên nhân gốc rễ (Root Cause) | Cách khắc phục (Resolution) | Nguyên tắc phòng ngừa (Prevention Rule) |
|:---:|:---:|---|---|---|---|
| 01 | 2026-10-06 | *Ví dụ mẫu: PySpark Iceberg Schema Mismatch khi thêm cột mới* | *Chưa kích hoạt Iceberg schema evolution trên Spark Session* | *Bổ sung cấu hình spark.sql.iceberg.schema-evolution=true* | *Luôn kiểm tra và đối chiếu schema trước khi chạy MERGE INTO* |
| 02 | 2026-10-06 | `UnicodeEncodeError: 'charmap' codec can't encode character` khi chạy script in tiếng Việt trên Windows | Console PowerShell Windows mặc định dùng encoding `cp1252` thay vì `utf-8` | Thêm `sys.stdout.reconfigure(encoding='utf-8')` ở đầu script | Luôn cấu hình UTF-8 cho `sys.stdout` trong tất cả các script Python chạy đa nền tảng |
| 03 | 2026-10-06 | `TypeError: make_branch_name() takes 1 positional argument` / `merge_branch_to_main` thiếu `spark` | Sai lệch signature hàm tiện ích trong `nessie_catalog_utils.py` | Kiểm tra định nghĩa hàm: `make_branch_name(prefix)` và `merge_branch_to_main(spark, branch)` | Luôn kiểm tra kỹ signature của các module tiện ích dùng chung trước khi gọi |
| 04 | 2026-10-06 | Lệnh chạy PySpark lần 2 bị treo/chạy rất lâu (`keep running`) | Thiếu `spark.stop()` trong `finally` block khiến JVM và port socket trong container bị chiếm dụng | Thêm khối `try ... finally: spark.stop()` trong `spark_generic_dynamic_processor.py` | Mọi script PySpark phải luôn đảm bảo gọi `spark.stop()` khi kết thúc để giải phóng tài nguyên container |
| 05 | 2026-10-06 | `docker exec` bị treo hoặc trả về `500 Internal Server Error` từ pipe `dockerDesktopLinuxEngine` | Docker Desktop Engine trên Windows WSL2 bị nghẽn named pipe khi container chạy quá tải RAM/CPU | Kiểm tra tình trạng Docker Desktop, giải phóng lệnh treo hoặc khởi động lại Docker Desktop nếu pipe bị khóa | Tránh spam lệnh `docker exec` khi tiến trình con chưa kết thúc; đặt timeout và monitor tài nguyên WSL |
| 06 | 2026-10-06 | Task `join_and_smoke_test` bị Failed với lỗi `ModuleNotFoundError: No module named 'trino'` | Container Airflow không cài đặt sẵn package python `trino` (chỉ có trong Superset/Backend) | Dùng Trino REST API `/v1/statement` qua thư viện chuẩn `urllib.request` của Python | Không import các package ngoài chưa có trong image Airflow; ưu tiên dùng REST API chuẩn |
| 07 | 2026-10-06 | Task `process_dynamic_silver_gold` bị FAILED: `queries from raw JSON/CSV files are disallowed when referenced columns only include corrupt record column` | File báo cáo PDF (`KPI_Q3_2026_TEST_NEW_CHART_RULES.pdf`) bị định tuyến nhầm vào nhánh `generic_dynamic`. Spark cố đọc binary PDF thành JSON dẫn đến lỗi `_corrupt_record` | Cập nhật `check_legacy_kpi_match` nhận diện đúng file tài liệu (.pdf, .docx...) vào `legacy_kpi`; bổ sung guard chặn file nhị phân trong Generic Processor | Luôn phân tách dứt khoát dữ liệu bảng cấu trúc (CSV/JSON) và tài liệu phi cấu trúc (PDF/Word) tại AI Router trước khi đưa vào Spark |

---

## 🔒 NGUYÊN TẮC BẢO TOÀN HỆ THỐNG
1. **Không xóa bỏ code cũ**: Toàn bộ luồng KPI và Registered API hiện tại phải được bảo toàn làm nhánh riêng, đảm bảo hệ thống đang chạy không bị gián đoạn.
2. **Deterministic Fallback**: Dù AI có lỗi, pipeline vẫn phải hoàn thành công việc nhờ cơ chế dự phòng rule-based / surrogate key.
3. **Mọi cập nhật tiến độ đều ghi vào file này**.
