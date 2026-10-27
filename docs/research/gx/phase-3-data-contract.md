# Giai đoạn 3 — Data Contract pilot

Ngày kiểm chứng: 2026-10-27  
Dataset: `education.teaching_progress`  
Contract: `1.0.0` (`pilot`)

## Kết luận

Data contract machine-readable đã được tạo cho Silver và Gold, gồm schema
evolution, freshness, null ratio, duplicate ratio, volume, range và
Silver–Gold reconciliation. Mỗi rule khai báo rõ engine tương thích.

Candidate grain `(ma_lop_hoc_phan, nam_hoc, hoc_ky)` phù hợp với snapshot demo
hiện tại nhưng **chưa được xác nhận bởi source owner trên dữ liệu production**.
Vì vậy uniqueness/duplicate-ratio chỉ là `warning`; loader chủ động từ chối nếu
ai đổi rule này thành `critical` trước khi `business_semantics_verified=true`.

## Artifact

- Registry: `lakehouse/gx/contract_registry.json`
- Contract: `lakehouse/gx/contracts/education.teaching_progress/1.0.0.json`
- Loader/safeguard: `lakehouse/gx_runtime/contracts.py`

Contract và registry được copy vào image GX, chỉ đọc từ Git artifact. Suite ở
giai đoạn 4 sẽ triển khai các rule này, không định nghĩa lại contract.

## Bằng chứng profiling thực tế

Các truy vấn chỉ đọc chạy qua Trino trên Iceberg/Nessie `main` ngày 2026-10-27.

Snapshot identity:

| Asset | Snapshot ID | Commit/thời điểm |
|---|---:|---|
| Nessie `main` | — | `07224a10eca333523113fff048aed0cdf329c466140e877f1c174aff3bf58fc8` |
| `lakehouse.silver.teaching_progress` | `4366971137478837827` | `2026-10-01 01:00:18.401 UTC` |
| `lakehouse.gold.teaching_progress_metrics` | `5052954023966271510` | `2026-10-20 08:25:55.404 UTC` |

Silver profile:

| Chỉ số | Kết quả |
|---|---:|
| Rows | 30 |
| Distinct `ma_ban_ghi` | 30 |
| Distinct candidate key | 30 |
| Duplicate candidate-key groups | 0 |
| Null record ID | 0 |
| Null course section | 0 |
| Null academic year | 0 |
| Null semester | 0 |
| Null progress | 0 |
| Oldest source update | `2025-07-20 02:00 UTC` |
| Newest source update | `2026-01-20 02:42 UTC` |

Gold profile và reconciliation:

| Chỉ số | Kết quả |
|---|---:|
| Gold rows | 30 |
| Distinct Gold candidate grain | 30 |
| Null source record ID | 0 |
| Null progress | 0 |
| Missing/mismatched Silver–Gold rows | 0 |

Các dòng mẫu cho thấy `ma_ban_ghi` đang được tạo theo dạng kết hợp course
section, academic year và semester trong fixture demo. Đây là bằng chứng về dữ
liệu demo, không phải bằng chứng nghiệp vụ production.

## Schema evolution policy

| Thay đổi | Chính sách |
|---|---|
| Thêm cột | Cho phép với warning |
| Xóa cột | Cấm |
| Đổi type | Cấm |
| Nullable → required | Phải tăng contract version |
| Rename cột | Phải tăng contract version |

## Rule policy

- Required schema và required-field null ratio: `critical`.
- Progress ngoài `[0,100]`: `critical`.
- Dataset rỗng: `critical`.
- Candidate-key duplicate ratio: `warning` cho tới khi xác nhận business grain.
- Freshness: `warning`, pilot threshold 8.760 giờ; phải thay bằng source SLA
  trước production.
- Silver–Gold count, key-set, value và lineage reconciliation: `critical`.

## Engine compatibility

- Silver schema/null/duplicate/volume/freshness/range: Spark và SQL.
- Gold rules: SQL/Trino.
- Reconciliation: Spark hoặc SQL.

Không giả định kết quả expectation giữa hai engine giống nhau. Giai đoạn 4 phải
test từng expectation trên từng engine đã khai báo.

## Kiểm thử

- Contract registry resolve đúng dataset/version.
- Có đủ các rule family bắt buộc.
- Mọi rule có ít nhất một supported engine.
- Ratio threshold bị giới hạn trong `[0,1]`.
- Uniqueness critical bị từ chối khi business grain chưa xác nhận.
- Schema evolution policy được kiểm tra rõ ràng.

## Điều kiện trước khi nâng uniqueness thành critical

1. Source owner xác nhận grain production bằng văn bản/data contract chính thức.
2. Profile nhiều snapshot và khoảng thời gian đại diện, không chỉ 30 dòng demo.
3. Xác minh late-arriving update và correction có tạo nhiều record cho cùng key.
4. Cập nhật `business_semantics_verified=true` trong contract qua review.
