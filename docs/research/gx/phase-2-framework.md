# Giai đoạn 2 - Xây dựng GX framework dùng chung

Ngày kiểm chứng: 2026-10-15  
Nhánh: `research/great-expectations-poc`

## Mục tiêu

Tạo lớp tích hợp Great Expectations (GX) dùng chung cho các dataset/layer của
lakehouse, thay vì viết logic GX trực tiếp trong từng DAG hoặc từng Spark job.
Giai đoạn này xây nền tảng thực thi và chính sách kết quả; chưa đưa GX vào luồng
publish dữ liệu thật.

## Kết quả đã thực hiện

### 1. GX project được lưu cùng mã nguồn

- Root trên host: `lakehouse/gx`.
- Root trong Airflow container: `/opt/airflow/gx`.
- Dùng `FileDataContext`; không cho phép tự động rơi về Ephemeral Context.
- Expectations, checkpoints và validation definitions là cấu hình có thể quản
  lý phiên bản.
- Validation results, Data Docs, biến cấu hình cục bộ và store ID được đặt dưới
  `uncommitted` hoặc được `.gitignore` để không đưa dữ liệu runtime/bí mật vào Git.

Các store hiện có:

| Store | Mục đích | Lưu trữ |
|---|---|---|
| `expectations_store` | Bộ luật kiểm tra | `lakehouse/gx/expectations` |
| `checkpoint_store` | Cấu hình chạy validation | `lakehouse/gx/checkpoints` |
| `validation_definition_store` | Liên kết batch và suite | `lakehouse/gx/validation_definitions` |
| `validation_results_store` | Kết quả mỗi lần chạy | `lakehouse/gx/uncommitted/validations` |
| Data Docs | Báo cáo HTML cục bộ | `lakehouse/gx/uncommitted/data_docs/local_site` |

### 2. Runner dùng chung cho Spark DataFrame

Hàm `validate_dataframe(...)` nhận:

- Spark DataFrame cần kiểm tra;
- một hoặc nhiều tên Expectation Suite;
- `dataset_id`, `data_layer`, `run_id`;
- metadata tùy chọn gồm `table_name` và `nessie_branch`.

Runner tự tạo hoặc tái sử dụng Spark datasource, DataFrame asset, batch
definition, validation definition và checkpoint. Tên tài nguyên được chuẩn hóa
để tránh tên không hợp lệ và tránh tạo trùng khi chạy lại.

Framework áp dụng fail-closed cho lỗi hạ tầng/cấu hình:

- không tìm thấy GX project: `GXProjectConfigurationError`;
- không tìm thấy suite: `GXSuiteNotFoundError`;
- GX không thực thi được: `GXExecutionError`.

Điều này ngăn pipeline hiểu nhầm việc GX không chạy là dữ liệu hợp lệ.

### 3. Mô hình kết quả chuẩn hóa

`GXValidationSummary` tách hai khái niệm:

- `raw_gx_success`: kết quả nguyên bản của GX;
- `success`: quyết định theo policy của lakehouse.

Kết quả còn chứa số expectation được đánh giá/thành công/thất bại, danh sách lỗi
critical/warning, suite, dataset, layer, run ID, table, Nessie branch và GX result
ID. `as_dict()` trả về cấu trúc tương thích JSON để sau này ghi log, XCom hoặc
metadata store.

### 4. Chính sách critical và warning

Mặc định:

- `critical`: validation thất bại và phải chặn bước publish;
- `warning`: GX vẫn ghi nhận thất bại nhưng policy không chặn pipeline;
- expectation không khai báo severity được xem là `critical`.

Policy được tách khỏi runner, nên có thể thay đổi mức chặn mà không sửa logic
thực thi GX.

### 5. Registry định tuyến suite

`SuiteRegistry` ánh xạ cặp `(dataset_id, data_layer)` sang danh sách suite. Thiết
kế này giữ business rule ở suite/registry, không nhúng vào runner. Ở giai đoạn
sau, orchestration sẽ tra registry để biết dataset mới cần chạy suite nào.

### 6. Docker Compose

Cả `airflow-webserver` và `airflow-scheduler` được cấu hình:

- mount `./lakehouse/gx:/opt/airflow/gx`;
- `GX_CONTEXT_ROOT=/opt/airflow/gx`;
- `GX_ANALYTICS_ENABLED=false`.

`docker compose config --quiet` đã thành công.

## Kiểm thử và nghiệm thu

### Test framework riêng

10/10 test đạt, bao phủ:

- nạp đúng File Data Context;
- thiếu context phải fail-closed;
- thiếu suite phải báo lỗi;
- critical failure chặn;
- warning failure không chặn nhưng vẫn giữ raw GX failure;
- chạy lặp lại tái sử dụng tài nguyên;
- summary chuyển được sang JSON;
- policy và suite registry.

Trong lúc kiểm thử phát hiện GX 1.24.0 không nhận chuỗi trực tiếp ở
`Checkpoint.run(run_id=...)`. Runner đã được sửa để dùng `RunIdentifier`, sau đó
toàn bộ test đạt.

### Hồi quy toàn project

- Compile framework: đạt.
- Airflow DB migration tạm phục vụ DAG test: đạt.
- Tổng cộng: **64/64 test đạt trong 35.647 giây**.
- Airflow DagBag vẫn nạp các DAG hiện có; không phát sinh lỗi import do GX.

Các warning còn quan sát được nhưng không làm test thất bại: API
`schedule_interval` của Airflow 2.9.2 đã deprecated, thiếu lệnh `ps` trong image,
Spark native Hadoop library fallback, và một số socket `ResourceWarning`. Đây
không phải regression của framework GX nhưng nên được xử lý ở hardening phase.

## Tiêu chí hoàn thành giai đoạn 2

- [x] Có GX File Data Context bền vững và mount được vào Airflow.
- [x] Có runner dùng chung cho Spark DataFrame.
- [x] Có kiểu lỗi rõ ràng và fail-closed.
- [x] Có kết quả chuẩn hóa, tương thích JSON.
- [x] Có policy critical/warning.
- [x] Có registry dataset/layer -> suites.
- [x] Test framework và regression đều đạt.

## Ngoài phạm vi giai đoạn 2

- Chưa tạo suite nghiệp vụ thật cho `teaching_progress` hoặc dataset khác.
- Chưa gắn runner vào DAG/Spark processor đang chạy.
- Chưa thay đổi thứ tự merge/publish của Silver và Gold.
- Chưa ghi kết quả GX vào Nessie/Iceberg metadata hoặc hệ thống quan sát tập
  trung.
- Chưa đo overhead GX trên dữ liệu đại diện production.

## Hướng sang giai đoạn 3

Chọn một dataset pilot (khuyến nghị `education.teaching_progress` Silver), tạo
suite thật từ data contract, đăng ký suite trong registry và đặt GX trước thao
tác merge/publish. Phải kiểm thử ít nhất ba đường đi: dữ liệu hợp lệ được publish,
critical failure bị chặn và warning failure vẫn cho phép publish nhưng có bằng
chứng quan sát được.
