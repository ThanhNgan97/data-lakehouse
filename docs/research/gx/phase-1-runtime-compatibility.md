# Giai đoạn 1 — GX runtime độc lập

Ngày cập nhật: 2026-10-26
Nhánh: `research/great-expectations-poc`

## Kết luận

Runtime Great Expectations (GX) đã được tách khỏi Airflow thành image
`lakehouse-gx-validator:1.24.0`. Airflow không còn cài GX, không mount GX project
và không giữ state validation. Giai đoạn này chỉ xác nhận runtime và các đường
kết nối; API/framework nghiệp vụ thuộc giai đoạn 2.

## Thành phần và phiên bản cố định

| Thành phần | Phiên bản |
|---|---:|
| Python | 3.10 |
| Java | OpenJDK 17 |
| Great Expectations | 1.24.0 |
| PySpark | 3.5.0 |
| SQLAlchemy | 2.0.44 |
| Trino Python client + SQLAlchemy dialect | 0.336.0 |
| Iceberg Spark runtime | 1.4.3 |
| Nessie Spark extensions | 0.77.1 |
| Hadoop AWS | 3.3.4 |

Nguồn dependency của runtime nằm tại
`lakehouse/gx_runtime/requirements.txt`; image nằm tại `gx.Dockerfile`.

## Ranh giới với Airflow

- `airflow.Dockerfile` không cài `great_expectations`.
- Airflow webserver/scheduler không mount `lakehouse/gx`.
- Airflow không cung cấp `GX_CONTEXT_ROOT` hoặc giữ GX result trên filesystem.
- Compose service `gx-validator` dùng profile `gx`, vì vậy không làm tăng thời
  gian khởi động stack mặc định.
- Container chạy bằng user không phải root (`uid=10001`) và nhận connection
  configuration/secret qua environment.

Đây là ranh giới runtime, chưa phải giao thức gọi service. Validation request,
result model và persistent store sẽ được hoàn thiện ở giai đoạn 2.

## Các probe

CLI thống nhất:

```text
python -m gx_runtime.smoke {imports|spark|trino|nessie-table|all}
```

### 1. Imports và Trino dialect

```powershell
docker compose --profile gx run --rm --no-deps gx-validator imports
```

Probe import GX, PySpark, SQLAlchemy và Trino, sau đó yêu cầu SQLAlchemy registry
nạp dialect `trino`. Nếu dialect/driver không tương thích, lệnh trả exit code 1.

### 2. Spark DataFrame

```powershell
docker compose --profile gx run --rm --no-deps gx-validator spark
```

Probe dùng GX ephemeral context chỉ cho kiểm thử runtime. Nó xác nhận:

- DataFrame có `progress` trong `[0,100]` phải pass.
- DataFrame có `progress=101` phải fail đúng như mong đợi.

### 3. Kết nối Trino qua SQLAlchemy

```powershell
docker compose --profile gx run --rm gx-validator trino
```

Probe tạo SQLAlchemy engine bằng dialect chính thức từ `trino[sqlalchemy]`, kết
nối tới Trino và thực thi `SELECT 1`. Không dùng package `sqlalchemy-trino` cũ.

### 4. Đọc Iceberg table trên Nessie branch cụ thể

Đặt branch và bảng rõ ràng, không dựa ngầm vào `main`:

```powershell
$env:GX_NESSIE_BRANCH='candidate_abc'
$env:GX_ICEBERG_TABLE='lakehouse.silver.teaching_progress'
docker compose --profile gx run --rm gx-validator nessie-table
```

Probe thực hiện theo thứ tự:

1. Gọi Nessie API và xác nhận reference tồn tại, đúng loại `BRANCH` và đúng tên.
2. Khởi tạo Spark catalog với `spark.sql.catalog.lakehouse.ref` bằng chính
   branch được yêu cầu.
3. Đọc/count bảng Iceberg.
4. Đọc metadata table `snapshots` và trả `snapshot_id` mới nhất.
5. Trả cả Nessie commit hash, branch, table và row count dưới dạng JSON.

Branch hoặc table không tồn tại, thiếu credential, kết nối lỗi hay không đọc
được snapshot đều làm probe trả exit code 1. Không có cơ chế skip/fallback sang
`main`.

## Quyết định cho SQL validation

PoC xác nhận đường kết nối SQLAlchemy/Trino là khả dụng ở mức runtime. Không kết
luận mọi GX expectation đều có hành vi tương đương giữa Spark và SQL. Giai đoạn
2 sẽ giữ một `SQL Validator` adapter boundary; expectation không tương thích sẽ
được triển khai bằng custom SQL thay vì ép dùng GX SQL datasource.

Trino catalog hiện được cấu hình với Nessie ref tĩnh (`main`). Vì cần định danh
branch theo từng validation request, probe branch-aware dùng Spark/Nessie catalog
trực tiếp. Không tuyên bố Trino đang cung cấp branch-per-query khi cấu hình hiện
tại chưa chứng minh điều đó.

## Ma trận nghiệm thu

| Tiêu chí | Cách chứng minh |
|---|---|
| GX chạy ngoài Airflow image | `gx.Dockerfile`; GX bị gỡ khỏi `airflow.Dockerfile` |
| Docker image GX độc lập | Compose service/profile `gx-validator` |
| Không phụ thuộc filesystem Airflow | Không mount thư mục Airflow/GX vào service |
| Smoke test Spark | Probe `spark` |
| Smoke test Trino | Probe `trino` chạy `SELECT 1` |
| SQLAlchemy Trino dialect | Probe `imports` nạp registry `trino` |
| Iceberg trên branch cụ thể | Probe `nessie-table` trả branch/hash/snapshot |
| Fail rõ khi hạ tầng sai | CLI trả exit code 1 và JSON error |

## Kết quả kiểm chứng

Đã chạy ngày 2026-10-26 trên Docker Desktop với stack lakehouse đang hoạt động:

| Kiểm tra | Kết quả |
|---|---|
| Build `lakehouse-gx-validator:1.24.0` | PASS |
| Import GX/PySpark/SQLAlchemy/Trino | PASS |
| SQLAlchemy dialect | `trino.sqlalchemy.dialect.TrinoDialect` |
| GX + Spark valid fixture | PASS |
| GX + Spark invalid fixture | FAIL_AS_EXPECTED |
| Trino qua SQLAlchemy `SELECT 1` | PASS |
| Nessie candidate branch | `ctu_ioc_teaching_progress_20261020_082429` |
| Nessie commit hash | `090afe43f5560d92faad8bc84e820c76de312c039f17d44166791cee2f0a1127` |
| Iceberg table | `lakehouse.silver.teaching_progress` |
| Iceberg snapshot ID | `4366971137478837827` |
| Số dòng đọc từ candidate branch | `30` |

Các giá trị branch/snapshot trên là bằng chứng của lần chạy, không phải default
để framework sử dụng lại. Mỗi validation request sau này phải cung cấp identity
của candidate hiện tại.

## Giới hạn thực thi

Các unit test và `docker compose config` có thể chạy không cần stack. Build image
và ba probe runtime cần Docker daemon; probe Trino/Nessie cần stack cùng branch
và table thực tế. Kết quả của mỗi lần chạy được in thành JSON để có thể lưu làm
bằng chứng kiểm chứng.
