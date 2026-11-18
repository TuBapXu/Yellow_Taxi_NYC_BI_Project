# B04 — Báo cáo Kỹ thuật Staging, Chất lượng dữ liệu & Đối soát Tài chính

| Mục | Nội dung |
|---|---|
| Task | B04 (Kế hoạch ETL Giai đoạn 2 — Nhóm 09) |
| Người thực hiện | nbinh (Nguyễn Ngọc Bình — 23120218) |
| Phụ thuộc | B03 (SQL kiểm chứng & audit DQ), S03 (nạp đủ 36 tháng) |
| Phạm vi dữ liệu | 36 file Parquet 01/2021–12/2023, **108.870.632 dòng** |
| Ngày chạy số liệu | 18/11/2026 |
| Kết luận | **ĐẠT** cả 2 tiêu chí nghiệm thu (mục 9) |

**Sản phẩm bàn giao**

| Sản phẩm (theo Kế hoạch GĐ2) | File |
|---|---|
| Bản thảo Báo cáo Phần Staging & DQ | `docs/B04_BAO_CAO_STAGING_DQ.md` (tài liệu này) |
| Bảng đối soát tài chính `reconciliation.csv` | `reports/b04/reconciliation.csv` (theo năm) + `reconciliation_by_file.csv` (36 file) + `reconciliation_staging_3way.csv` (3 chiều qua staging) + `reconciliation_summary.md` |
| Rà soát ca cực trị & kiểm chứng bộ cờ | `sql/b04_dq_review_queries.sql` → `reports/b04/dq_review/X01–X06.csv` |
| Script tái lập | `scripts/b04_reconciliation.py`, `scripts/b04_dq_review.py` |

---

## 1. Kiến trúc tầng Staging

### 1.1. Luồng dữ liệu

```
Parquet gốc (36 file, 2 schema)
   │  S01: PyArrow đọc theo batch, gắn lineage (SHA-256 file, số dòng, batch UUID)
   ▼
staging.raw_yellow_trips        19 cột nguồn ở dạng TEXT + 5 cột lineage (giữ nguyên trạng, không ép kiểu)
   │  B01: stg_yellow_trips_typed — ép kiểu an toàn từng cột, ghi mã lỗi chuyển kiểu
   ├──► stg_yellow_trips_rejects ──► staging.quarantine   (dòng KHÔNG chuyển kiểu được, kèm lý do + giá trị gốc)
   ▼
staging.stg_yellow_trips        dòng chuyển kiểu được, đúng kiểu Data Contract v1.2
   │  B02: macro dq_rules.sql — tính mã lỗi độc lập cho từng nhóm rule
   ▼
staging.stg_trips_flagged       + 3 cờ usable, has_dq_issue, data_quality_flags, attribute_quality_flags, rule version
   │  T02: 11 phép JOIN vào 8 Dimension, khóa thiếu → 0
   ▼
dds."FactTaxiTrip"              1 dòng / 1 chuyến, giữ 100% dòng (Preserve & Flag)
```

Các view staging là `materialized: view` (dbt), riêng đầu vào mock là `ephemeral`. Mọi tầng đều mang theo bộ lineage
`source_file_name`, `source_file_sha256`, `source_row_number`, `load_batch_id`, `loaded_at_utc`. Nhờ đó bất kỳ dòng
nào trong Fact cũng truy được về đúng file và số dòng gốc trong Parquet (xem ví dụ ở mục 6).

### 1.2. Hai schema nguồn được hợp nhất

| Nhóm file | Đặc điểm | Chuẩn hóa |
|---|---|---|
| 01/2021–01/2023 (25 file, schema v1) | `airport_fee`; passenger/rate kiểu `double`; Vendor/LocationID `int64` | Đưa về schema staging chung |
| 02/2023–12/2023 (11 file, schema v2) | `Airport_fee` (viết hoa); passenger/rate `int64`; Vendor/LocationID `int32` | `Airport_fee → airport_fee`, cùng kiểu staging |

Kiểm chứng: đối soát 3 chiều ở mục 5.3 chạy trên **1 file mỗi schema** (2021-01 và 2023-12), đều khớp tuyệt đối.

### 1.3. Staging là vùng đệm tạm (thay đổi so với kế hoạch ban đầu)

Kế hoạch ban đầu giữ cả 108 triệu dòng trong staging (~25–30 GB). Theo **RFC-002 v2 (phương án Hybrid)** đã được nhóm chốt,
`staging.raw_yellow_trips` chỉ là vùng đệm **cho 1 file tại một thời điểm**: S03 xóa staging trước mỗi file, PA3 xóa ở W2/W6.
Hệ quả cho B04: không thể đối soát "staging so với Fact" trên đủ 36 file bằng cách đọc staging. B04 giải quyết như sau
(chi tiết mục 5):

1. Đối soát **Nguồn (Parquet gốc) = Fact + Quarantine** cho đủ 36 file, tính lại trực tiếp từ Parquet theo đúng quy tắc B01.
2. Đối soát **3 chiều Nguồn = Raw = Staging = Fact** trên file mẫu của từng schema: nạp lại file vào staging trong một
   transaction, đọc tổng qua toàn bộ chuỗi view B01/B02, rồi **ROLLBACK**. Database không thay đổi gì.

---

## 2. Quy tắc làm sạch (Cleansing Rules — B01)

| Nhóm cột | Kiểu staging | Quy tắc | Khi không chuyển được |
|---|---|---|---|
| `vendor_id`, `passenger_count`, `rate_code_id`, `pu/do_location_id`, `payment_type` | `INT` | Chấp nhận `1` và `1.0` → 1; `1.5` không được cắt phần lẻ | Quarantine (`TYPE_CAST_ERROR` / `OVERFLOW_ERROR` / `NON_FINITE_NUMBER`) |
| `pickup_datetime`, `dropoff_datetime` | `TIMESTAMP` | Giữ NULL nguồn; chuỗi phải bắt đầu bằng số và parse được | Quarantine (`UNPARSABLE_TIMESTAMP`) |
| `trip_distance` | `NUMERIC(18,6)` | Không làm tròn trước khi phân nhóm cự ly | Quarantine |
| 9 cột tiền | `NUMERIC(18,2)` | Chuẩn hóa về cent, làm tròn **nửa xa số 0**; nếu giá trị bị đổi thì gắn `MONEY_ROUNDED` và giữ giá trị gốc | Quarantine |
| `store_and_fwd_flag` | `VARCHAR(1)` | Chuẩn hóa `Y`/`N` (bỏ khoảng trắng, không phân biệt hoa thường); khác thì NULL + cờ `INVALID_STORE_AND_FWD_FLAG` | — |
| `trip_id` | `CHAR(32)` | `MD5(source_file_sha256 ‖ '_' ‖ source_row_number)`, ổn định qua mọi lần nạp | — |
| `trip_duration_seconds` | `NUMERIC(20,6)` | `EPOCH(dropoff − pickup)`, giữ phần giây lẻ, không có phép chia | — |

Nguyên tắc: **NULL nguồn giữ là NULL**, không âm thầm biến lỗi chuyển kiểu thành NULL. Dòng có lỗi chuyển kiểu đi vào
quarantine, còn dòng bất thường về nghiệp vụ vẫn vào Fact và chỉ bị gắn cờ.

**Kết quả trên 108.870.632 dòng thật:**

| Chỉ số | Kết quả |
|---|---|
| Dòng vào quarantine (lỗi chuyển kiểu) | **0** |
| Giá trị tiền phải làm tròn về cent (`MONEY_ROUNDED`) | **0** trên cả 9 cột (dữ liệu TLC 2021–2023 luôn có ≤ 2 chữ số thập phân) |
| Số giá trị NULL từng cột tiền: Nguồn = Fact | Khớp 9/9 cột |

Bộ quy tắc ép kiểu vẫn cần thiết: nó đã được kiểm thử trên bộ ca biên (`mock_cast_edge_cases.csv`), gồm 1.5, NaN/Inf,
tràn số, timestamp hỏng và ngày 2009. Nó là hàng rào cho các file mới khi chạy định kỳ từ 2024 trở đi.

---

## 3. Khung quản trị chất lượng dữ liệu (DQ Framework — B02)

### 3.1. Nguyên tắc "Preserve & Flag" và bộ cờ

Không xóa dòng nào. Mỗi dòng mang 3 cờ usable độc lập. Mỗi KPI chỉ lọc theo cờ liên quan đến nó (ví dụ vận tốc dùng
time + distance usable, doanh thu đủ điều kiện dùng amount usable), nên một lỗi tiền không làm mất chuyến khỏi phép đếm sản lượng.

| Cờ | TRUE khi (Contract v1.2 mục 2.2) | Mã lỗi khi FALSE |
|---|---|---|
| `IsTimeUsable` | Hai timestamp có giá trị; ngày thuộc 31/12/2020–01/01/2024; dropoff > pickup; thời lượng ≤ 24 giờ | `MISSING_TIME`, `DATE_OUT_OF_DOMAIN`, `TIME_REVERSED`, `DURATION_OVER_24H` |
| `IsDistanceUsable` | 0 < cự ly ≤ 100 dặm | `MISSING_DISTANCE`, `NEGATIVE_DISTANCE`, `ZERO_DISTANCE`, `DISTANCE_OUTLIER` |
| `IsAmountUsable` | (1) total/fare/tip có giá trị, total > 0, fare ≥ 0, tip ≥ 0; (2) total ≤ 5.000; (3) total + 0,01 ≥ fare + tip + tolls | `MISSING_*_AMOUNT`, `NEGATIVE_TOTAL_AMOUNT`, `ZERO_TOTAL_AMOUNT`, `NEGATIVE_FARE`, `NEGATIVE_TIP`, `AMOUNT_OVER_5000`, `INCONSISTENT_TOTAL` |
| `HasDQIssue` | = NOT (time AND distance AND amount usable) | — |

Cảnh báo thuộc tính (`AttributeQualityFlags`) tách riêng khỏi 3 cờ usable: `MISSING/INVALID_PASSENGER_COUNT`,
`MISSING/UNMAPPED_VENDOR`, `MISSING/UNMAPPED_RATE_CODE`, `MISSING/INVALID_STORE_AND_FWD_FLAG`, `MONEY_ROUNDED`.
Các cảnh báo này không làm dòng bị loại khỏi KPI tiền.

### 3.2. Kiểm chứng bộ cờ trên 108 triệu dòng (X05)

Cách kiểm: tính lại từng điều kiện của Contract v1.2 **trực tiếp từ các measure trong Fact**, độc lập với macro dbt, rồi so
với cờ đã lưu:

| Cờ | Dòng vi phạm điều kiện | Vi phạm nhưng vẫn usable | Không vi phạm nhưng bị loại |
|---|---:|---:|---:|
| IsTimeUsable | 108.528 | **0** | **0** |
| IsDistanceUsable | 1.760.917 | **0** | **0** |
| IsAmountUsable | 799.249 | **0** | **0** |
| HasDQIssue | 2.509.685 (2,31%) | **0** | **0** |

→ Bộ cờ khớp **100%** với quy tắc: không có dòng nào lọt cờ, cũng không có dòng sạch nào bị loại oan.

### 3.3. Tần suất mã lỗi theo năm file nguồn (B03 · DQ02)

| Mã lỗi (usable) | 2021 | 2022 | 2023 | Tổng | % dữ liệu |
|---|---:|---:|---:|---:|---:|
| ZERO_DISTANCE | 407.812 | 574.059 | 773.457 | 1.755.328 | 1,61% |
| NEGATIVE_FARE | 139.326 | 252.900 | 381.650 | 773.876 | 0,71% |
| NEGATIVE_TOTAL_AMOUNT | 139.714 | 255.706 | 376.883 | 772.303 | 0,71% |
| INCONSISTENT_TOTAL | 139.440 | 255.034 | 376.696 | 771.170 | 0,71% |
| TIME_REVERSED | 59.496 | 32.035 | 15.569 | 107.100 | 0,10% |
| ZERO_TOTAL_AMOUNT | 7.237 | 6.309 | 5.999 | 19.545 | 0,02% |
| NEGATIVE_TIP | 2.625 | 3.417 | 1.788 | 7.830 | 0,01% |
| DISTANCE_OUTLIER | 2.756 | 1.529 | 1.304 | 5.589 | 0,01% |
| DATE_OUT_OF_DOMAIN | 310 | 531 | 68 | 909 | <0,01% |
| DURATION_OVER_24H | 103 | 346 | 212 | 661 | <0,01% |
| AMOUNT_OVER_5000 | 7 | 64 | 7 | 78 | <0,01% |

| Cảnh báo thuộc tính | Tổng | Ghi chú |
|---|---:|---|
| MISSING_PASSENGER_COUNT / MISSING_RATE_CODE / MISSING_STORE_AND_FWD_FLAG | 4.156.354 mỗi mã | Chính là khối khuyết đồng loạt (mục 3.4) |
| INVALID_PASSENGER_COUNT | 2.050.306 | Chủ yếu passenger = 0 |
| UNMAPPED_RATE_CODE | 358.812 | Mã 99 chưa có trong seed mapping → RateCodeKey = 0 |
| UNMAPPED_VENDOR | 192.329 | Vendor ngoài 1/2 → VendorKey = 0 |

Xu hướng đáng chú ý: lỗi tiền âm (NEGATIVE_FARE) và cự ly 0 tăng đều qua các năm, trong khi TIME_REVERSED giảm mạnh
(59 nghìn năm 2021 xuống 16 nghìn năm 2023).

### 3.4. Khối khuyết đồng loạt — tiêu chí nghiệm thu B03 (DQ03)

| Năm file | Dòng | Khối khuyết | Tỷ lệ |
|---|---:|---:|---:|
| 2021 | 30.904.308 | 1.478.695 | 4,78% |
| 2022 | 39.656.098 | 1.368.303 | 3,45% |
| 2023 | 38.310.226 | 1.309.356 | 3,42% |
| **Tổng** | **108.870.632** | **4.156.354** | **3,8177%** |

Khớp **tuyệt đối** với baseline khảo sát (4.156.354 dòng = 3,8177%). Các dòng này được giữ nguyên trong Fact và ánh xạ về
Key = 0. Doanh thu đủ điều kiện của khối này là $125,1 triệu; nếu xóa các dòng này thì doanh thu sẽ bị hụt đúng bằng số đó.

---

## 4. Đối chiếu số liệu khảo sát trong Kế hoạch GĐ2 (mục 2.4) — X06

| Mô tả trong kế hoạch | Số kế hoạch | Số thực tế trong Fact | Kết luận |
|---|---:|---:|---|
| Dòng thời gian âm (dropoff ≤ pickup) | 107.100 | 107.100 | ✅ Khớp tuyệt đối |
| Dòng treo trên 1 ngày | 661 | 661 | ✅ Khớp tuyệt đối |
| Dòng cự ly bằng 0 | ~1,75 triệu | 1.755.328 | ✅ Khớp |
| Khối khuyết đồng loạt | 4.156.354 (3,8177%) | 4.156.354 (3,8177%) | ✅ Khớp tuyệt đối |
| Ca cực trị ~133 triệu USD năm 2022 | có | Có: file 2022-12, dòng 3.334.939 (mục 6.3) | ✅ Đã bị cờ loại |

---

## 5. Đối soát tài chính & sản lượng

### 5.1. Phương pháp

- **Nguồn:** đọc lại trực tiếp 36 file Parquet gốc bằng PyArrow. Áp dụng **đúng quy tắc của B01**: giá trị float được đổi sang chuỗi `str(x)` như S01 làm, sau đó ép `numeric(18,2)` với làm tròn nửa xa số 0 (tái hiện bằng `Decimal.quantize(ROUND_HALF_UP)`). Tổng được cộng bằng **số nguyên cent** để không có sai số dấu phẩy động.
- **Fact:** `SUM` theo `SourceFileName` cho 9 cột tiền, cộng thêm số dòng `staging.quarantine` của từng file.
- **Phép so:** `footer = Fact + Quarantine` (số dòng) và `Tiền nguồn = Tiền Fact` cho cả 9 cột. Quarantine bằng 0, nên tiền phải khớp tuyệt đối.
- Toàn bộ chạy trong một snapshot `REPEATABLE READ`, chỉ đọc. Thời gian chạy khoảng 5 phút (`scripts/b04_reconciliation.py`).

### 5.2. Kết quả theo năm (`reports/b04/reconciliation.csv`)

| Năm | File | Dòng nguồn (footer) | Dòng Fact | Quarantine | Lệch dòng | Tổng tiền nguồn (TotalAmount) | Tổng tiền Fact | Lệch |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2021 | 12 | 30.904.308 | 30.904.308 | 0 | **0** | 608.700.670,92 | 608.700.670,92 | **0,00** |
| 2022 | 12 | 39.656.098 | 39.656.098 | 0 | **0** | 859.397.945,19 | 859.397.945,19 | **0,00** |
| 2023 | 12 | 38.310.226 | 38.310.226 | 0 | **0** | 1.090.383.411,89 | 1.090.383.411,89 | **0,00** |
| **Tổng** | **36** | **108.870.632** | **108.870.632** | **0** | **0** | **2.558.482.028,00** | **2.558.482.028,00** | **0,00** |

Ngoài `TotalAmount`, 8 cột tiền còn lại (fare, extra, MTA, tip, tolls, improvement, congestion, airport fee) cũng **lệch 0,00**
ở cả 36 file. Chi tiết từng file nằm trong `reconciliation_by_file.csv`.

| Kiểm tra | Kết quả |
|---|---|
| Footer Parquet = số dòng đọc được (36 file) | PASS |
| Footer = Fact + Quarantine; 9 cột tiền Nguồn = Fact (từng file, từng cent) | PASS — 0/36 file lệch |
| Số giá trị NULL từng cột tiền: Nguồn = Fact | PASS — 9/9 cột |
| 3 chiều Nguồn = Raw = Staging = Fact (mục 5.3) | PASS — 2/2 file mẫu |

### 5.3. Đối soát 3 chiều qua Staging (`reconciliation_staging_3way.csv`)

| File | Schema | Footer | Raw | Reject | Staging (flagged) | Fact | TotalAmount Nguồn = Staging = Fact |
|---|---|---:|---:|---:|---:|---:|---:|
| yellow_tripdata_2021-01 | v1 | 1.369.769 | 1.369.769 | 0 | 1.369.769 | 1.369.769 | 23.935.896,09 (cả 3) |
| yellow_tripdata_2023-12 | v2 | 3.376.567 | 3.376.567 | 0 | 3.376.567 | 3.376.567 | 96.372.563,85 (cả 3) |

Cả 9 cột tiền đều bằng nhau ở 3 tầng. Hai file được nạp bằng chính hàm định dạng của S01 (`format_series_for_staging`),
đi qua đủ chuỗi view `raw → stg_yellow_trips_typed → stg_yellow_trips → stg_trips_flagged`, sau đó ROLLBACK.

### 5.4. Tổng tiền nguồn và tổng tiền đủ điều kiện (CB08, DQ04)

Theo Kế hoạch GĐ2 (bảng 2.5, CB08), số liệu tiền được công bố tách 2 khái niệm:

| Khái niệm | Định nghĩa | 2021 | 2022 | 2023 | Tổng |
|---|---|---:|---:|---:|---:|
| Tổng tiền nguồn | SUM(TotalAmount), mọi dòng | 608,70 tr | 859,40 tr | 1.090,38 tr | **2.558,48 tr USD** |
| Tổng tiền đủ điều kiện | SUM(TotalAmount) WHERE IsAmountUsable | 609,20 tr | 862,64 tr | 1.099,36 tr | **2.571,20 tr USD** |
| Chênh lệch | nguồn − đủ điều kiện | −0,50 tr | −3,24 tr | −8,97 tr | **−12,71 tr USD** |

**Tổng tiền đủ điều kiện lớn hơn tổng tiền nguồn**, và đây không phải lỗi. Tổng tiền nguồn có cộng cả các giao dịch âm
(hoàn tiền, tranh chấp, hủy), tổng khoảng −16,57 triệu USD. Tổng tiền đủ điều kiện loại các giao dịch này ra, đồng thời
loại 78 ca trên 5.000 USD (+3,82 triệu USD). Cộng thêm vài nhóm nhỏ khác (total bằng 0, lệch cấu thành), kết quả thuần
là −12,71 triệu USD. Số tiền bị loại tách theo từng mã lỗi nằm trong `reports/b03/results/DQ04.csv`.

---

## 6. Rà soát ca cực trị

### 6.1. Fare âm (X01)

| Năm file | Chuyến fare âm | Tổng fare âm (USD) | Trong đó total cũng âm | Còn usable |
|---|---:|---:|---:|---:|
| 2021 | 139.326 | −1,68 tr | 138.715 | **0** |
| 2022 | 252.900 | −181,58 tr ¹ | 252.842 | **0** |
| 2023 | 381.650 | −7,82 tr | 375.068 | **0** |
| **Tổng** | **773.876** (0,71%) | −191,08 tr | 766.625 | **0** |

¹ Gần như toàn bộ là 2 dòng lỗi tràn số ở file 2022-12 (mục 6.3).

Theo hình thức thanh toán: **Dispute** chiếm 442.033 chuyến, **No charge** 156.806, **Cash** 165.975 và **Credit card** chỉ 580.
Như vậy fare âm chủ yếu là bút toán đảo hoặc hoàn tiền ở các giao dịch tranh chấp, miễn phí và tiền mặt, không phải lỗi đồng hồ
trên giao dịch thẻ. **100%** chuyến fare âm đều đã bị `IsAmountUsable = FALSE`.

### 6.2. Tip trên chuyến trả thẻ (X02, X03)

| Năm file | Chuyến trả thẻ | Tip âm (còn usable) | Tip > fare | Tip > 100 USD (còn usable) | Tip lớn nhất | Tỷ lệ tip (usable) |
|---|---:|---:|---:|---:|---:|---:|
| 2021 | 22.480.382 | 64 (0) | 52.092 | 421 (420) | 1.140,44 | 23,54% |
| 2022 | 30.085.763 | 135 (0) | 66.286 | 579 (579) | 1.400,16 | 23,22% |
| 2023 | 29.856.932 | 159 (0) | 46.155 | 532 (532) | 984,30 | 22,19% |
| **Tổng** | **82.423.077** | **358 (0)** | **164.533** | **1.532 (1.531)** | **1.400,16** | **22,83%** |

- Mọi chuyến có tip âm đều bị loại.
- Tip trên 100 USD được **giữ là usable**. Contract không đặt trần cho tip, và các chuyến này có cấu thành hợp lệ (ví dụ fare $47, tip $1.400,16, total $1.447,96 ở file 2022-06 dòng 2.566.129). Tổng cộng chỉ 1.531 trên 82,4 triệu chuyến thẻ nên gần như không ảnh hưởng tỷ lệ tip. Nhóm có thể cân nhắc thêm cảnh báo (không loại) ở rule v1.3.

**Tip ghi nhận trên chuyến không trả thẻ (X03):**

| Hình thức | Chuyến | Có tip > 0 | Tỷ lệ |
|---|---:|---:|---:|
| Cash | 20.838.398 | 4.239 | 0,02% |
| Dispute | 863.456 | 8.756 | 1,01% |
| No charge | 589.335 | 3.086 | 0,52% |
| **Unknown / Missing (payment = 0)** | 4.156.354 | **2.793.901** | **67,22%** |

**Phát hiện quan trọng:** 67% chuyến thuộc khối payment = 0 (khối khuyết đồng loạt) **có tip điện tử**. Điều này cho thấy
phần lớn khối này thực chất là chuyến trả bằng thẻ hoặc qua ứng dụng, chỉ bị mất mã thanh toán. Tỷ lệ tip theo định nghĩa
của contract (chỉ tính Credit card) **không bao gồm khối này**. Báo cáo MR01 cần ghi rõ giới hạn đó.

### 6.3. Danh sách ca cực trị kèm lineage (X04, trích)

| Nhóm | File | Dòng gốc | Hình thức | Fare | Tip | Total | Cờ |
|---|---|---:|---|---:|---:|---:|---|
| Total lớn nhất | 2021-09 | 394.007 | No charge | 818.283,44 | 0 | 818.286,74 | AMOUNT_OVER_5000 |
| Total lớn nhì | 2022-01 | 429.860 | Dispute | 401.092,32 | 0 | 401.095,62 | AMOUNT_OVER_5000 |
| **Ca tràn số ~133 triệu** | **2022-12** | **3.334.939** | Unknown | **−133.391.414,00** | **+133.391.363,53** | −47,17 | NEGATIVE_FARE; NEGATIVE_TOTAL_AMOUNT; ZERO_DISTANCE |
| Ca tràn số thứ hai | 2022-12 | 3.335.232 | Unknown | −44.463.768,92 | +44.463.786,70 | 21,08 | NEGATIVE_FARE; ZERO_DISTANCE |
| Total âm nhất | 2022-04 | 2.095.264 | No charge | −2.564,00 | 0 | −2.567,80 | INCONSISTENT_TOTAL; NEGATIVE_FARE; NEGATIVE_TOTAL_AMOUNT |
| Tip thẻ lớn nhất | 2022-06 | 2.566.129 | Credit card | 47,00 | 1.400,16 | 1.447,96 | (usable) |
| Tip âm nhất | 2023-05 | 1.749.474 | Dispute | −8,60 | −411,00 | −423,60 | INCONSISTENT_TOTAL; NEGATIVE_FARE; NEGATIVE_TIP; NEGATIVE_TOTAL_AMOUNT |

Ca "133 triệu USD" trong kế hoạch thực chất là **một cặp fare âm và tip dương gần như triệt tiêu nhau** (total chỉ −47,17).
Nếu không có cờ, tổng tip sẽ bị đội thêm 133 triệu USD và tổng fare bị kéo âm cùng mức đó. Rule `NEGATIVE_FARE` đã chặn
đúng ca này. Toàn bộ 50 ca đầy đủ nằm trong `reports/b04/dq_review/X04.csv`.

### 6.4. Lệch cấu thành cước (DQ04)

31.329.066 dòng có tổng 8 thành phần (fare + extra + MTA + tip + tolls + improvement + congestion + airport) lệch `TotalAmount`
hơn 1 cent. Tổng lệch −61,93 triệu USD, nghĩa là tổng thành phần lớn hơn total. Rule `INCONSISTENT_TOTAL` theo contract chỉ
kiểm **cận dưới** `total ≥ fare + tip + tolls`, nên phần lớn các dòng này vẫn usable. Đây là đặc điểm ghi nhận phụ phí của TLC
theo từng giai đoạn. Rule không loại các dòng này, và báo cáo theo dõi chúng như một chỉ số giám sát.

---

## 7. Phát hiện ngoài phạm vi & khuyến nghị cho nhóm

| # | Phát hiện | Ảnh hưởng | Khuyến nghị |
|---|---|---|---|
| 1 | Chuyến có ngày 2024–2030 (ngoài miền DQ) giờ có **DateKey thật**, vì DimDate đã mở rộng đến 2030 (RFC v2). Contract v1.2 mục 2.2 lại ghi "ngoài miền → DateKey = 0". | 8 chuyến (2024: 6, 2028: 1, 2029: 1), đều đã gắn `DATE_OUT_OF_DOMAIN` | Nâng **Contract v1.3**: DateKey theo lịch DimDate, miền DQ chỉ quyết định cờ. Báo cáo theo ngày nên lọc `IsTimeUsable`. |
| 2 | `staging.raw_yellow_trips` vẫn ở chế độ **UNLOGGED** sau S03. Khi Docker tắt đột ngột, Postgres đã tự xóa sạch bảng này. | Không mất dữ liệu (staging là vùng tạm), nhưng flag `--keep-last-staging` của S03 không còn tác dụng | Thêm bước `SET LOGGED` cho staging ở cuối S03, hoặc ghi rõ trong runbook rằng staging có thể trống |
| 3 | 67% khối payment = 0 có tip điện tử (mục 6.2) | Tỷ lệ tip (chỉ tính thẻ) không phản ánh khối 4,15 triệu chuyến | Ghi chú giới hạn ở MR01; cân nhắc thêm phân tích riêng cho khối này |
| 4 | 1.531 chuyến thẻ có tip > 100 USD vẫn usable | Không đáng kể với tỷ lệ tip | Có thể thêm cảnh báo thuộc tính ở rule v1.3 (không loại) |
| 5 | Tên gọi KPI: contract dùng "tổng tiền nguồn / đủ điều kiện", còn B03 trước đây dùng "gộp / thuần" | Dễ hiểu nhầm khi ghép báo cáo | Báo cáo tổng dùng thống nhất cách gọi của Kế hoạch GĐ2 bảng 2.5 (như mục 5.4) |

---

## 8. Cách chạy lại

Cần Docker đang chạy và Fact đã nạp đủ. Cả hai script **chỉ đọc** database (bước 3 chiều dùng transaction + ROLLBACK).

```powershell
docker compose exec -T mage sh -c "cd /home/src && python3 scripts/b04_reconciliation.py"   # ~5 phút
docker compose exec -T mage sh -c "cd /home/src && python3 scripts/b04_dq_review.py"        # ~35 phút (X05 quét 108M dòng)
```

Kết quả được ghi vào `reports/b04/` và log nằm trong `logs/b04_*.log`. Script trả exit code ≠ 0 nếu có phép đối soát hoặc kiểm chứng nào FAIL.

---

## 9. Đối chiếu tiêu chí nghiệm thu B04 (Kế hoạch GĐ2)

| Tiêu chí | Bằng chứng | Kết quả |
|---|---|---|
| Số liệu tài chính đối soát khớp giữa Staging và Fact | Mục 5: Nguồn = Fact trên 36/36 file, 9/9 cột tiền, lệch 0,00 USD; 3 chiều Nguồn = Raw = Staging = Fact trên file mẫu cả 2 schema | ✅ ĐẠT |
| Báo cáo mô tả chi tiết, minh bạch các quy tắc gắn cờ | Mục 2–3: quy tắc làm sạch, điều kiện từng cờ, danh mục mã lỗi; kiểm chứng độc lập 108M dòng, 0 dòng lọt cờ (X05) | ✅ ĐẠT |
| Đối soát tổng tiền và sản lượng theo năm | `reports/b04/reconciliation.csv` (mục 5.2, 5.4) | ✅ |
| Rà soát ca cực trị (fare âm, tip thẻ) | Mục 6, `reports/b04/dq_review/X01–X04.csv` | ✅ |
