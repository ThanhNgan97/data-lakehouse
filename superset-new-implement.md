# SYSTEM PROMPT / EXECUTION DIRECTIVE: TỐI ƯU HÓA GOLD LAYER & DASHBOARD SUPERSET

## 1. MỤC TIÊU VÀ NGUYÊN TẮC CỐT LÕI
Agent tiếp nhận tài liệu này cần thực hiện 2 nhiệm vụ chính:
1. **Refactor Gold Layer (Data Lakehouse)**: Giữ nguyên toàn bộ schema/aggregation hiện có, bổ sung các trường dữ liệu mang tính **ra quyết định (Decision-driven Fields)**.
2. **Chuẩn hóa Layout & Nội dung Superset Dashboard**: Quy định các thành phần bắt buộc và cấu trúc hiển thị theo mô hình 3 tầng (Executive - Tactical - Operational) nhằm giải quyết triệt để bài toán dashboard thiếu giá trị hành động.

---

## 2. NHIỆM VỤ 1: ĐIỀU CHỈNH TẦNG GOLD (DATA MARTS / AGGREGATION)

### 2.1. Yêu cầu giữ nguyên (Non-breaking changes)
- Giữ nguyên tất cả các cột kích thước (Dimensions), khóa chính (Primary Keys), khóa ngoại (Foreign Keys) và metric cơ bản (`total_amount`, `record_count`, `sum_quantity`,...).
- Không đổi tên (rename) các cột hiện hữu để tránh làm hỏng các downstream pipeline hoặc reporting query cũ.

### 2.2. Nhóm trường quyết định (Decision Fields) bắt buộc bổ sung
Mỗi bảng tổng hợp ở tầng Gold cần được làm giàu thêm bằng 4 nhóm trường sau:

#### A. Cờ cảnh báo hành động (Action Flags & Alert Status)
- Chuyển đổi số liệu thô thành trạng thái hành động cụ thể cho nghiệp vụ:
  - `action_priority`: `HIGH`, `MEDIUM`, `LOW` (xác định thứ tự ưu tiên xử lý).
  - `health_status`: `HEALTHY`, `AT_RISK`, `CRITICAL` (dựa trên SLA, công nợ hoặc KPI).
  - `churn_risk_flag`: `TRUE`/`FALSE` hoặc điểm rủi ro `risk_score` $[0.0, 1.0]$.
  - `reorder_trigger_flag`: `TRUE`/`FALSE` (tồn kho dưới mức an toàn, cần đặt hàng ngay).

#### B. Ngữ cảnh so sánh (Variance & Benchmark Context)
Một con số đơn lẻ không mang giá trị hành động nếu không có mốc đối chiếu:
- **So với mục tiêu (Target/Budget)**:
  - `target_value`: Mục tiêu định mức được gán cho entity/thời điểm.
  - `target_achievement_pct`: `(actual_value / target_value) * 100`.
  - `target_variance_amount`: `actual_value - target_value`.
- **So với quá khứ (Time Comparison)**:
  - `mom_variance_pct`: Tỷ lệ tăng trưởng so với tháng trước (Month-over-Month).
  - `wow_variance_pct`: Tỷ lệ tăng trưởng so với tuần trước (Week-over-Week).
  - `yoy_variance_pct`: Tỷ lệ tăng trưởng so với cùng kỳ năm trước.

#### C. Phân khúc & Phân loại định hướng (Actionable Segmentation)
- `revenue_tier`: Hạng mục A/B/C theo nguyên lý Pareto (80/20).
- `lifecycle_stage`: `NEW`, `ACTIVATED`, `DORMANT`, `CHURNED`.
- `margin_efficiency_category`: `HIGH_VOLUME_LOW_MARGIN`, `STAR_PRODUCT`, `MONEY_DRAIN`.

#### D. Thời gian suy thoái & SLA (Aging & Inactivity Indicators)
- `days_since_last_activity`: Số ngày kể từ lần tương tác/mua hàng/phát sinh gần nhất.
- `overdue_days`: Số ngày quá hạn công nợ/xử lý ticket.
- `sla_breach_flag`: `TRUE`/`FALSE`.

---

## 3. NHIỆM VỤ 2: QUY CHUẨN THIẾT KẾ SUPERSET DASHBOARD

Superset Dashboard không được phép là một "Data Dump" (chỉ liệt kê dữ liệu thô). Mọi dashboard cho từng domain phải tuân theo cấu trúc phân tầng trực quan từ trên xuống dưới.

### 3.1. Cấu trúc Layout chuẩn (Từ trên xuống dưới)

```
┌────────────────────────────────────────────────────────────────────────┐
│ 1. HEADER & GLOBAL FILTERS                                             │
│    - Date Range (bắt buộc) | Tổ chức/Chi nhánh | Trạng thái rủi ro    │
├────────────────────────────────────────────────────────────────────────┤
│ 2. ROW 1: EXECUTIVE KPI CARDS (3 - 5 Cards)                            │
│    [ Big Number + % vs Target / MoM Trend ]                            │
├────────────────────────────────────────────────────────────────────────┤
│ 3. ROW 2: TACTICAL & EXCEPTION CHARTS (Middle Section)                 │
│    [ Phân tích Xu hướng & Gap ]  |  [ Danh sách Cảnh báo Nguy cơ / Top ]│
│    (Trend Line vs Target)        |  (Bar chart phân loại rủi ro)       │
├────────────────────────────────────────────────────────────────────────┤
│ 4. ROW 3: ROOT CAUSE DRILL-DOWN                                        │
│    [ Heatmap / Cohort / Treemap phân rã theo Dimensions ]               │
├────────────────────────────────────────────────────────────────────────┤
│ 5. ROW 4: OPERATIONAL ACTION TABLE (Bottom Section)                    │
│    [ Bảng chi tiết các đối tượng gắn Action Flag kèm nút/link xử lý ]  │
└────────────────────────────────────────────────────────────────────────┘
```

### 3.2. Chi tiết từng khu vực trên Dashboard

#### Khu vực 1: Global Native Filters
- **Vị trí**: Filter Bar bên trái hoặc trên cùng.
- **Thành phần**:
  - `Time Range`: Default về `Last 30 Days` hoặc `Current Quarter`.
  - `Action Priority`: Cho phép lọc nhanh chỉ xem `HIGH` / `CRITICAL`.
  - `Business Unit / Department`: Lọc theo phạm vi quyền hạn.

#### Khu vực 2: Executive Summary (Big Number with Trendline)
- **Mục tiêu**: Người ra quyết định nhìn 5 giây là biết hệ thống/nghiệp vụ đang ổn hay gặp sự cố.
- **Thành phần**: Tối đa 4 - 5 thẻ (Cards):
  1. *Metric Kết quả chính (Outcome)*: Kèm % hoàn thành KPI (`target_achievement_pct`).
  2. *Metric Hiệu quả (Efficiency/Margin)*: Kèm mức tăng giảm so với kỳ trước.
  3. *Metric Rủi ro (Risk Count)*: Số lượng entities đang ở trạng thái `CRITICAL` / `SLA_BREACH`.
  4. *Metric Dự báo hoặc Run-rate*: Xu hướng kết thúc chu kỳ.

#### Khu vực 3: Tactical / Exception Detection
- **Mục tiêu**: Chỉ ra "vấn đề đang nằm ở đâu".
- **Biểu đồ cần có**:
  - **Biểu đồ Xu hướng (Line / Mixed Timeseries)**: Vẽ đường `Actual Value` đè lên đường `Target Value` hoặc dải phân vị kỳ vọng để thấy rõ khoảng cách lệch (variance).
  - **Biểu đồ Cảnh báo tập trung (Horizontal Bar / Pareto)**: Top 10 đối tượng chiếm 80% rủi ro hoặc 80% doanh thu; phân bổ các nhóm trạng thái `health_status`.

#### Khu vực 4: Operational Action Table
- **Mục tiêu**: Cung cấp danh sách cụ thể để nhân viên vận hành xử lý ngay lập tức.
- **Biểu đồ**: Table View có phân trang (Pagination), tuyệt đối không để table dài vô tận.
- **Cột bắt buộc phải có**:
  - Tên Entity / Mã đối tượng (`Customer ID`, `Order ID`, `SKU`,...).
  - Giá trị rủi ro / doanh số liên quan.
  - Cột `Action Flag` (format tô màu Conditional Formatting: Đỏ cho `CRITICAL`, Vàng cho `WARNING`).
  - Hướng dẫn hành động ngắn gọn (`Recommended Action`: "Gọi tái kích hoạt", "Dừng xuất kho", "Kiểm toán lại đơn giá").

---

## 4. CHECKLIST NGHIỆM THU DÀNH CHO AGENT (VALIDATION CRITERIA)

Trước khi xác nhận hoàn thành, Agent phải tự kiểm tra các tiêu chí sau:

- [ ] **Bảo toàn Gold Schema**: Không có bảng hoặc cột nào sẵn có bị xóa hoặc đổi tên.
- [ ] **Tính hành động của Metric**: Mỗi metric sinh ra đều trả lời được: *"Nếu số này tăng/giảm thì ai phải hành động gì?"*.
- [ ] **Loại bỏ Vanity Metrics**: Không để các biểu đồ đếm số lượng vô nghĩa (như Total Raw Events, Total Hits) ở các vị trí trung tâm.
- [ ] **Quy tắc phối màu Superset**:
  - Màu **Đỏ/Cam**: Chỉ dành cho rủi ro, cảnh báo, giảm sút tiêu cực.
  - Màu **Xanh lá**: Đạt/vượt KPI mục tiêu.
  - Màu trung tính (Xanh dương/Xám): Thống kê diễn tiến chung.
- [ ] **Tối ưu hiệu năng Query**: Các chart trên Superset phải query trực tiếp vào bảng Gold aggregate sẵn hoặc view tối ưu, không query aggregate lồng `GROUP BY` trên toàn bộ bảng transaction hàng chục triệu dòng.