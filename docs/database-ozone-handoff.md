# Bàn giao tích hợp Database → Airbyte → Apache Ozone

## Mục tiêu và trạng thái hiện tại

Người vận hành kết nối một database bằng giao diện Database của portal, chọn bảng
qua `db-provisioner`, chạy Airbyte sync và xem file landing bằng WinSCP kết nối
Ozone S3 Gateway. Database không gọi S3 trực tiếp. Hiện Airbyte và bridge chưa
chạy; tài liệu này là hợp đồng triển khai/kiểm thử, không phải chứng nhận E2E.

Không thay thế các luồng file upload và API hiện có. Dùng một database/bảng thử,
tài khoản chỉ đọc, không mở CDC hoặc đồng bộ bảng thật trong lần kiểm thử đầu.

## Thành phần đã có trong repo

- `frontend/src/pages/UserDatabase.jsx`: nhận bundle, gửi đến bridge
  `/api/v1/airbyte/onboard-source`, hiển thị connection/job và tiến trình Airflow.
- `lakehouse/airbyte/internal/airbyte/bridge.go`: tạo Airbyte source, destination
  S3/Parquet và connection, kích hoạt sync; khi job thành công, trigger
  `universal_lakehouse_pipeline` với `context_id` và `airbyte_job_id`.
- `lakehouse/dags/universal_lakehouse_pipeline.py`: nhận Airbyte context từ
  `staging/<context_id>/`, chạy nhánh relational.
- `lakehouse/spark/spark_relational_context_processor.py`: đọc Parquet landing
  từ Ozone bằng `s3a://`, ghi các bảng Iceberg Silver/Gold.

Compose có profile Airbyte `v0.64.0`, nhưng chưa có container Airbyte và chưa
được xác nhận là một triển khai đầy đủ. Không chạy `docker compose up` toàn stack
chỉ để thử database. Docker Desktop hiện được cấp khoảng 7,6 GiB RAM, gần hết
do các dịch vụ lakehouse đang chạy; cần đủ tài nguyên hoặc host Airbyte riêng.

## Hợp đồng kết nối

1. Source DB: PostgreSQL/MySQL/SQL Server được tool hỗ trợ. Chỉ chọn bảng test,
   tài khoản đọc tối thiểu. Kiểm tra từ **nơi chạy Airbyte connector**, không
   chỉ từ trình duyệt hoặc máy chạy `db-provisioner`.
2. Airbyte: triển khai bản hoạt động đầy đủ, kiểm tra API/worker và một sync
   job thực tế. Bridge phải gọi được Airbyte API và Airflow API. Giao diện portal
   đang mặc định gọi bridge tại `http://localhost:9090`; `localhost` là máy
   **mở trình duyệt**, nên nếu bridge đặt trên host khác phải đặt
   `VITE_DB_PROVISIONER_BRIDGE_URL` phù hợp.
3. Destination: S3-compatible endpoint của Ozone, bucket
   `university-lakehouse`, vùng `staging/<context_id>/<table>/`, định dạng
   Parquet/Snappy. Nếu connector cùng Docker network với Ozone, endpoint là
   `http://s3g:9878`. Nếu connector chạy trên Docker Desktop nhưng ngoài network,
   kiểm tra `http://host.docker.internal:9878`. Nếu chạy máy khác, dùng địa chỉ
   host truy cập được qua mạng riêng/TLS; không dùng `localhost:9878` trên máy
   khác. Kiểm tra path-style S3 và quyền bucket bằng PUT/HEAD/GET một key test.
   Không lưu access/secret key trong repo hoặc gửi qua chat.
4. WinSCP: profile Amazon S3 trỏ tới **cùng Gateway và bucket**. Sau khi Airbyte
   job thành công, mở `/university-lakehouse/staging/<context_id>/`, vào thư mục
   bảng và tìm object `.parquet` thực tế. Tên file do connector sinh ra, không
   giả định luôn là `part_0.parquet`. WinSCP liệt kê/tải file; editor văn bản
   không đọc được nội dung Parquet nhị phân.

## Trình tự bàn giao và tiêu chí PASS

| Giai đoạn | Bằng chứng bắt buộc |
| --- | --- |
| Sẵn sàng | Airbyte API/worker, bridge và DB source reachable; destination S3 kiểm tra PUT/HEAD/GET thành công. |
| Onboarding | Bundle chỉ chứa bảng test; portal trả `source_id`, `destination_id`, `connection_id`, `job_id`. Đây mới là **đã kích hoạt sync**, chưa phải đã ghi dữ liệu. |
| Sync | Airbyte job ở trạng thái `SUCCEEDED`, số dòng ghi > 0; ghi lại thời gian, context và bảng. |
| Ozone | Liệt kê đúng prefix `staging/<context_id>/<table>/`; ít nhất một `.parquet` mới có kích thước > 0; đọc lại file và so số dòng/khóa mẫu với DB. Đây mới là PASS DB → S3 Gateway. |
| Processing | Airflow run `airbyte__<job_id>` đi qua nhánh relational; Spark đọc đúng landing key; Silver/Gold và truy vấn Trino PASS nếu cần E2E. |

Không đánh dấu “đã lên Ozone” chỉ dựa trên `connection_id`, `job_id`, phản hồi
onboard `SUCCESS`, hoặc việc thấy thư mục cũ trong WinSCP. Cần **key cụ thể,
thời gian, kích thước, số dòng và kết quả GET/đọc Parquet** của lần sync đó.

## Việc còn phải hoàn thiện trước khi giao cho người dùng cuối

- Cài Airbyte đầy đủ và kiểm tra tương thích API với bridge Go hiện có; profile
  Compose hiện tại chưa được chứng minh là có thể vận hành sync.
- Bỏ fallback S3 credential hard-code trong bridge; thiếu biến môi trường phải
  báo lỗi cấu hình. Quản lý secret ngoài repo và giới hạn quyền bucket.
- Sau Airbyte job `SUCCEEDED`, bridge/backend phải liệt kê và đọc lại object
  Parquet thuộc **job đó** trước khi portal hiển thị PASS Ozone. Trả về bucket,
  prefix/key, kích thước, số dòng và trạng thái xác minh (không trả secret).
- Giao diện Database hiển thị rõ: `Đã tạo kết nối` → `Đang sync` →
  `Đã xác minh Parquet trên Ozone` → `Airflow xử lý xong`, kèm đường dẫn WinSCP
  có thể sao chép. Không dùng cùng một nhãn “thành công” cho bốn trạng thái.
- Kiểm thử một bảng test từ đầu đến cuối, lưu bằng chứng; sau đó mới mở rộng
  quyền và số bảng.
