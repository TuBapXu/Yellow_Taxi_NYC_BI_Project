# DATA CONTRACT V1 — GIAO DIỆN TẦNG STAGING (TASK B01)

**Dự án:** Kho dữ liệu & BI NYC Yellow Taxi (2021–2023) — Nhóm 09
**Task:** B01 — Data Contract & dbt Staging Transformations (nbinh)
**Nguồn chuẩn:** `docs/DATA_CONTRACT_AND_MOCK_GUIDE.md` (Data Contract v1.2). Tài liệu này là bản triển khai cho tầng staging; khi hai tài liệu khác nhau, v1.2 được ưu tiên và tài liệu này phải được cập nhật theo.
**Trạng thái:** Đã cài đặt và kiểm thử (mock + 2 file thật). **Chờ cả ba thành viên (ttu, nbinh, stinh) xác nhận** mapping nguồn, miền ngày, bảng khóa, seed fixture và expected trước khi nghiệm thu (Contract mục 5).

Phạm vi bàn giao B01 theo Contract mục 5.1: **mapping hai schema (§3), chuyển kiểu và quarantine (§5), miền ngày (§6), expected fixture (§8), chuẩn hóa chạy được (§9).**

---

## 1. Luồng dữ liệu

```
staging.raw_yellow_trips  (S01 – mọi cột nghiệp vụ dạng VARCHAR + 5 cột lineage)
        │
        ▼
stg_yellow_trips_source   (ephemeral: raw thật, hoặc seed giả lập khi b01_use_mock = true)
        │
        ▼
stg_yellow_trips_typed    (view: ép kiểu an toàn + mảng cast_errors theo cột)
        ├──► stg_yellow_trips          (view: dòng ép kiểu thành công → đầu vào B02 stg_trips_flagged)
        └──► stg_yellow_trips_rejects  (view: dòng lỗi) ──► dbt run-operation load_quarantine ──► staging.quarantine
```

Bất biến đối soát (test `assert_stg_reconciles_with_raw`):
**COUNT(raw) = COUNT(stg_yellow_trips) + COUNT(stg_yellow_trips_rejects)**

## 2. Quy ước chung

| Quy ước | Nội dung |
|---|---|
| Tên | Staging/meta dùng `snake_case` chữ thường. DDS dùng `PascalCase` có quote (`dds."FactTaxiTrip"`) |
| Schema dbt | Macro `generate_schema_name` dùng đúng `+schema` (staging / dds), không sinh tiền tố kiểu `staging_dds` |
| Preserve & Flag | Staging không xóa dòng bất thường nghiệp vụ (âm, 0, ngoại lai, đảo thời gian, ngoài miền ngày). Chỉ lỗi **kỹ thuật** (không ép kiểu được) mới tách sang quarantine |
| NULL | NULL nguồn được giữ NULL. Chuỗi rỗng, hoặc chuỗi chữ `\N` do loader S01 ghi ra, cũng được coi là NULL nguồn. Giá trị có nhưng không ép được thì **không** âm thầm thành NULL mà vào quarantine |
| Giờ | Timestamp chuyến giữ giờ địa phương nguồn (`TIMESTAMP` không múi giờ); `loaded_at_utc` theo UTC |

## 3. Mapping hai schema nguồn

| Nhóm nguồn | `source_schema_version` (manifest S01) | Đặc điểm Parquet | Chuẩn hóa vào raw/staging |
|---|---|---|---|
| 01/2021–01/2023 (25 file) | `v1_2021_2022` | `airport_fee`; passenger/rate: double; Vendor/LocationID: int64; store flag: string | Giữ tên; mã nguyên double `1.0` → `'1'` |
| 02/2023–12/2023 (11 file) | `v2_2023` | `Airport_fee`; passenger/rate: int64; Vendor/LocationID: int32; store flag: large_string | `Airport_fee → airport_fee`; cùng schema staging |

- Validator S01 kiểm tên và kiểu **thực tế**; khoảng tháng chỉ để đối chiếu.
- Hai tên airport fee cùng có mặt → dừng file với `SCHEMA_ALIAS_CONFLICT`.
- Thiếu cột bắt buộc → lỗi mức file, không tự thêm NULL hay bỏ cột.
- Đã kiểm thực tế: 2021-01 (v1) và 2023-02 (v2) cùng về một schema staging, không mất `airport_fee`.

| Cột raw (S01) | Cột Parquet v1 | Cột Parquet v2 | Cột staging |
|---|---|---|---|
| `vendor_id` | `VendorID` | `VendorID` | `vendor_id` |
| `tpep_pickup_datetime` / `tpep_dropoff_datetime` | như raw | như raw | `pickup_datetime` / `dropoff_datetime` |
| `rate_code_id` | `RatecodeID` | `RatecodeID` | `rate_code_id` |
| `pu_location_id` / `do_location_id` | `PULocationID` / `DOLocationID` | như v1 | giữ tên raw |
| `airport_fee` | `airport_fee` | `Airport_fee` | `airport_fee` |
| các cột còn lại | trùng tên | trùng tên | trùng tên |

## 4. Giao diện `staging.stg_yellow_trips`

| Cột | Kiểu | NULL | Quy tắc |
|---|---|:-:|---|
| `trip_id` | CHAR(32) | NO | `md5(source_file_sha256 \|\| '_' \|\| source_row_number)`, hex chữ thường, số dòng dạng thập phân; = `TripKey` ở Fact |
| `vendor_id` | INT | YES | TRY_CAST; `'1.0'` → 1 |
| `pickup_datetime` | TIMESTAMP | YES | TRY_CAST; giữ giờ địa phương nguồn, giữ giây lẻ |
| `dropoff_datetime` | TIMESTAMP | YES | như trên |
| `passenger_count` | INT | YES | TRY_CAST; `'1.0'` → 1, `'1.5'` → quarantine |
| `trip_distance` | NUMERIC(18,6) | YES | Không làm tròn về 2 chữ số. Giá trị có hơn 6 chữ số thập phân bị **phát hiện** bởi test cảnh báo `warn_trip_distance_precision_over_6` để review |
| `rate_code_id` | INT | YES | TRY_CAST; `'1.0'` → 1, `'1.5'` → quarantine; 99 giữ nguyên |
| `store_and_fwd_flag` | VARCHAR(1) | YES | `upper(trim(x))` ∈ {Y, N}; giá trị khác → NULL |
| `pu_location_id`, `do_location_id` | INT | YES | Giữ mã nguồn hoặc NULL (264/265 giữ nguyên); chỉ ánh xạ key 0 khi tạo FK |
| `payment_type` | INT | YES | TRY_CAST (0 giữ nguyên) |
| `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `total_amount` | NUMERIC(18,2) | YES | TRY_CAST; làm tròn đến cent theo kiểu nửa ra xa 0 (mặc định của PostgreSQL numeric) |
| `trip_duration_seconds` | NUMERIC(20,6) | YES | `EXTRACT(EPOCH FROM dropoff − pickup)`; giữ giây lẻ; có thể âm; NULL nếu thiếu mốc. **Không có phép chia nên không thể lỗi chia cho 0** |
| `store_and_fwd_flag_raw` | VARCHAR | YES | Giá trị gốc đã trim, để B02 gắn `INVALID_STORE_AND_FWD_FLAG` |
| `trip_distance_raw` | VARCHAR | YES | Giá trị gốc, để review độ chính xác cự ly |
| `money_rounded_columns` | TEXT | YES | Các cột tiền có giá trị gốc khác giá trị sau khi làm tròn cent, sắp alphabet, nối `;`; NULL nếu không có. B02 dùng để gắn `MONEY_ROUNDED` |
| `source_file_name` | VARCHAR(255) | NO | Lineage |
| `source_file_sha256` | CHAR(64) | NO | Lineage |
| `source_row_number` | BIGINT | NO | Lineage (1..N liên tục theo file) |
| `load_batch_id` | VARCHAR(64) | NO | Lineage |
| `loaded_at_utc` | TIMESTAMP | NO | Lineage |

Các cột `is_time_usable`, `is_distance_usable`, `is_amount_usable`, `has_dq_issue`, `data_quality_flags`, `attribute_quality_flags`, `quality_rule_version` do **B02** bổ sung tại `stg_trips_flagged` (Contract mục 2.2).

## 5. Chuyển kiểu an toàn & quarantine

PostgreSQL không có `TRY_CAST`. B01 dùng `pg_input_is_valid()` (PostgreSQL 16) trong `macros/safe_cast.sql`, nên câu truy vấn **không bao giờ ném lỗi ép kiểu**.

| `quarantine_reason` | Khi nào |
|---|---|
| `TYPE_CAST_ERROR` | Không phải số hoặc số nguyên hợp lệ (vd: `'abc'`, mã có phần lẻ `'1.5'`) |
| `OVERFLOW_ERROR` | Là số nhưng vượt kiểu đích (INT, NUMERIC(18,2), NUMERIC(18,6)) |
| `NON_FINITE_NUMBER` | `NaN`, `Infinity` |
| `UNPARSABLE_TIMESTAMP` | Ngày giờ không hợp lệ (`2021-02-30`, `now`, `epoch`…) |

- Một dòng có nhiều lỗi chỉ tạo **một** bản ghi quarantine. `quarantine_reason` gồm các mã không lặp, sắp alphabet, nối bằng `;`.
- `raw_payload` (JSONB) chứa đủ 19 giá trị gốc, cộng thêm khóa `_cast_errors` ghi chi tiết `MÃ:cột`.
- `dbt run-operation load_quarantine` chạy idempotent: không ghi trùng cùng `(source_file_sha256, source_row_number, quarantine_reason)`.
- Thêm `--args '{prune_stale: true}'` để xóa những dòng không còn là lỗi theo rule hiện hành.

## 6. Miền ngày

| Biến dbt (`dbt_project.yml`) | Giá trị | Ý nghĩa |
|---|---|---|
| `dq_date_domain_start` | `2020-12-31` | Biên dưới, **bao gồm** |
| `dq_date_domain_end` | `2024-01-01` | Biên trên, **bao gồm** (đến hết 01/01/2024) |
| `dq_rule_version` | `v1.2` | Ghi vào `quality_rule_version` ở flagged và Fact |

- Staging **không** loại dòng ngoài miền.
- B02 gắn `DATE_OUT_OF_DOMAIN` và đặt time unusable.
- S02/T02 gán DateKey = 0 nhưng TimeKey vẫn tính theo giờ/phút nguồn.
- DimDate dùng cùng miền: 1.097 ngày + key 0 = 1.098 dòng.

## 7. Quy ước khóa 0 (tóm tắt Contract mục 3.1)

Staging **giữ nguyên mã nguồn hoặc NULL**. Việc ánh xạ về key 0 chỉ xảy ra khi ghép Fact; FK trong Fact không bao giờ NULL.

| Dimension | Khóa | Về key 0 khi |
|---|---|---|
| DimDate | `YYYYMMDD` | timestamp NULL hoặc ngoài miền |
| DimTime | `hour*60 + minute + 1` (1–1440) | timestamp NULL |
| DimZone | `LocationID` (1–265; 264/265 là thành viên riêng) | NULL hoặc ngoài danh mục |
| DimVendor | `VendorID` đã xác minh | NULL hoặc chưa xác minh (vd: 5, 6) |
| DimPaymentType | `code` đã xác minh và khác 0 | mã 0, NULL hoặc chưa ánh xạ |
| DimRateCode | `RateCodeID` đã xác minh (fixture: 99 → 99) | NULL hoặc chưa xác minh |
| DimPassengerGroup | 1 SOLO; 2 GROUP_2_4; 3 GROUP_5_PLUS | NULL, ≤0 hoặc >9 |
| DimDistanceBand | 1 ZERO; 2 SHORT (0,2]; 3 MEDIUM (2,5]; 4 LONG (5,100]; 5 OUTLIER | NULL hoặc âm |

## 8. Dữ liệu giả lập & bộ kỳ vọng

| Seed | Nội dung |
|---|---|
| `mock_yellow_trips.csv` | 18 dòng contract (TC01–TC06), sao từ `data/mock/mock_yellow_trips.csv`, thêm `source_row_number` |
| `mock_cast_edge_cases.csv` | 24 ca biên (xem danh sách ngay dưới bảng) |
| `mock_expected_stg_yellow_trips.csv` | Kỳ vọng B01 cho từng dòng: vào stg hay quarantine, lý do, passenger, store flag, duration, vendor, rate, distance, `money_rounded_columns` |
| `mock_expected_flagged.csv` | Kỳ vọng **B02/T02** cho 18 dòng contract: 3 cờ usable, `has_dq_issue`, `data_quality_flags`, `attribute_quality_flags` và đủ 11 khóa Fact |

24 ca biên trong `mock_cast_edge_cases.csv`:
- passenger `1.0` / `1.5`; rate `1.0` / `1.5`;
- ngày 30/02; giá trị `now`; pickup NULL; ngày ngoài miền; biên ngày 31/12/2020 và 01/01/2024;
- `1e20` tràn kiểu; `NaN`; vendor tràn INT; nhiều lỗi trên 1 dòng;
- store flag `' y '` → Y, `X` → NULL; chuỗi `\N` → NULL;
- duration đúng 24h có giây lẻ;
- cự ly 1.999999 / 2.000001 / 100.000001 / 2.0000001;
- tiền `12.345` → 12.35 và `-0.005` → -0.01.

- Bộ kỳ vọng được tính bằng một script Python viết **độc lập** với SQL, theo đúng rule của contract.
- `mock_expected_flagged.csv` đã được đối chiếu **khớp 18/18 dòng** với bảng kỳ vọng ở Contract mục 4.
- Kỳ vọng trên mock: **34 dòng vào `stg_yellow_trips`, 8 dòng vào quarantine.**
- Test `assert_mock_matches_expected` phải trả 0 dòng lệch.
- Test cảnh báo `warn_trip_distance_precision_over_6` phải bắt **đúng 1** dòng (2.0000001). Trên mock, kết quả WARN=1 là đúng thiết kế.

## 9. Cách chạy

```bash
cd /home/src/dbt_project
# Mock
dbt seed --profiles-dir .
dbt run  --select staging --vars '{b01_use_mock: true}' --profiles-dir .
dbt test --select staging --vars '{b01_use_mock: true}' --profiles-dir .
# Dữ liệu thật (luôn chạy sau cùng để view trỏ lại raw thật)
dbt run  --select staging --profiles-dir .
dbt test --select staging --profiles-dir .
dbt run-operation load_quarantine --args '{prune_stale: true}' --profiles-dir .
```

Trên Windows: bấm đúp `RUN_B01.bat` ở thư mục gốc dự án. Log được ghi vào `logs/b01_*.log`.

## 10. Tiêu chí nghiệm thu B01

1. Cả ba thành viên thống nhất hợp đồng này (tên cột, kiểu, mapping hai schema, miền ngày, khóa 0, quarantine, expected). **Đang chờ.**
2. `dbt run` và `dbt test` của staging chạy pass trên mock, bao gồm so khớp kỳ vọng.
3. Không có lỗi chia cho 0 hay lỗi ép sai kiểu trên dữ liệu thật; đối soát số dòng khớp với raw.

## 11. Vấn đề phát hiện ở tầng trước (cần stinh xử lý)

- **S01 ghi NULL thành chuỗi `\N`.** Trong `export_parquet_chunks.py`, lệnh `to_csv(na_rep='\\N', escapechar='\\')` escape luôn dấu `\`, nên lệnh `COPY` lưu chuỗi 2 ký tự `\N` thay vì NULL. Điều này trái Contract nguyên tắc 4 (raw phải giữ đúng dữ liệu nguồn).
  - B01 đã tự xử lý bằng cách coi `\N` là NULL.
  - S01 vẫn nên sửa tận gốc rồi nạp lại raw (dùng `force_reload`).
