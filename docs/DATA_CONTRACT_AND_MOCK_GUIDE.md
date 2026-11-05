# DATA CONTRACT V1.2 & HƯỚNG DẪN KIỂM THỬ GIẢ LẬP (MOCK TEST GUIDE)
**Dự án:** Kho dữ liệu & BI Phân tích lộ trình NYC Yellow Taxi (2021 – 2023)  
**Nhóm thực hiện:** Nhóm 09 (Phạm Lê Thanh Tú, Nguyễn Ngọc Bình, Kiều Đỗ Song Tinh)  
**Phiên bản:** 1.2 — Bổ sung schema nguồn, miền ngày và khóa dimension; chờ nhóm xác nhận  
**Mục tiêu:** Thống nhất schema, khóa, quy tắc chất lượng và kết quả kiểm thử giữa ba thành viên. Phân công, lịch và deadline thực hiện theo `KE_HOACH_ETL_GIAI_DOAN_2_NHOM_09.pdf`.

---

## 1. NGUYÊN TẮC THIẾT KẾ BẤT BIẾN (CORE PRINCIPLES)

1. **Quy ước đặt tên (Naming Conventions):**
   * **Tầng Staging:** Dùng chữ thường `snake_case` (vd: `stg_yellow_trips`, `trip_duration_seconds`, `is_time_usable`).
   * **Tầng DDS / DWH:** Dùng `PascalCase` (vd: `FactTaxiTrip`, `PickupDateKey`, `TripDistanceMiles`).
2. **Quy ước Khóa mặc định (Surrogate Key = 0):**
   * Mọi bảng Dimension trong schema `dds` bắt buộc có bản ghi mặc định **`Key = 0`** đại diện cho trạng thái `Unknown / Missing / Unmapped`.
   * Khóa ngoại tại `FactTaxiTrip` **tuyệt đối không được chứa NULL**. Nếu dữ liệu nguồn bị khuyết hoặc chưa ánh xạ được, Fact **bắt buộc gán FK = 0**.
3. **Nguyên tắc "Bảo toàn & Gắn cờ" (Preserve & Flag):**
   * Tuyệt đối không xóa bỏ các dòng dữ liệu bất thường hoặc bị khuyết thuộc tính ở tầng Staging.
   * Mọi bản ghi nguồn hợp lệ về mặt kỹ thuật đều được nạp vào Fact; các bài toán phân tích/KPI sẽ dùng bộ cờ điều kiện (`IsTimeUsable`, `IsDistanceUsable`, `IsAmountUsable`) để lọc dữ liệu sạch tương ứng.
4. **Raw và quarantine:** Raw giữ dữ liệu nguồn đọc được cùng lineage. Dòng không chuyển kiểu được vào quarantine có nguyên nhân và giá trị gốc; dòng bất thường nghiệp vụ vẫn giữ trong Fact. Đối soát từng phiên bản file đã đọc đủ: `Nguồn = Fact + quarantine chưa nạp`; `Staging chuẩn hóa = Flagged = Fact` trên cùng phạm vi. File lỗi đọc/schema không được đánh dấu thành công.
5. **Phiên bản file:** Cùng nội dung đổi tên không nạp thêm. Cùng file logic/tháng đổi nội dung phải thay phiên bản qua manifest; không đồng thời tính cả bản cũ và mới. Chỉ công bố bản mới sau kiểm tra; thất bại thì bản cũ tiếp tục hiệu lực. Không thay thế theo năm timestamp.
6. **Quản lý cấu trúc:** stinh phụ trách dimensions; ttu phụ trách cấu trúc và nạp Fact. Khi dbt dựng lại bảng phải kiểm lại khóa, constraint và index. Zone 264/265 là thành viên nghiệp vụ riêng, không thay bằng key 0.
7. **Mapping dùng chung:** nbinh/stinh thống nhất bảng mapping tên/kiểu cột và seed dimension có phiên bản; ttu dùng đúng bộ khóa để ghép Fact. Không cấp lại khóa theo thứ tự dòng hoặc lần chạy. NULL nguồn là `MISSING`; mã có giá trị nhưng chưa ánh xạ là `UNMAPPED`; giữ raw code/trạng thái dù cả hai cùng về FK 0.
8. **Trạng thái batch:** `SUCCESS` khi kiểm tra/đối soát đạt và không còn quarantine chưa xử lý; `COMPLETED_WITH_QUARANTINE` khi đã xử lý hết nhưng còn quarantine; `FAILED` khi lỗi kỹ thuật khiến xử lý chưa hoàn tất. Hoàn tất xử lý không đồng nghĩa đạt nghiệm thu.

---

## 2. ĐẶC TẢ TẦNG STAGING & BỘ CỜ CHẤT LƯỢNG (DQ FLAGS)

### 2.1. Giao diện đầu ra chuẩn hóa và gắn cờ

Luồng: `staging.raw_yellow_trips → staging.stg_yellow_trips → staging.stg_trips_flagged`. Bảng dưới mô tả đầu ra cuối; các cờ DQ và phiên bản rule được bổ sung tại `stg_trips_flagged`.

**Mapping schema nguồn được chấp nhận:**

| Nhóm nguồn đã kiểm tra | Đặc điểm | Chuẩn hóa |
| :--- | :--- | :--- |
| 01/2021–01/2023 (25 file) | `airport_fee`; passenger/rate: `double`; Vendor/LocationID: `int64`; store flag: `string` | Tên và kiểu staging theo bảng dưới |
| 02/2023–12/2023 (11 file) | `Airport_fee`; passenger/rate: `int64`; Vendor/LocationID: `int32`; store flag: `large_string` | `Airport_fee → airport_fee`; về cùng schema staging |

Validator kiểm tên/kiểu thực tế; khoảng tháng chỉ để đối chiếu, không quyết định schema. Manifest ghi `source_schema_version`. Hai tên airport fee cùng tồn tại → dừng file với `SCHEMA_ALIAS_CONFLICT`. Thiếu cột bắt buộc hoặc schema chưa hỗ trợ → lỗi mức file, chờ xác nhận mapping; không tự thêm NULL/bỏ cột. `int32`/`int64` về INT khi trong miền giá trị; passenger/rate `1.0 → 1`, `1.5 → quarantine`, NULL giữ NULL. `string`/`large_string` về cùng kiểu chuỗi logic.

| Tên cột Staging | Kiểu dữ liệu | Cho phép NULL | Nguồn gốc & Quy tắc chuyển đổi |
| :--- | :--- | :---: | :--- |
| `trip_id` | `CHAR(32)` | **NO** | `MD5(source_file_sha256 \|\| '_' \|\| source_row_number)`; hash hex chữ thường, số dòng dạng thập phân; tại Fact đổi tên thành `TripKey` |
| `vendor_id` | `INT` | YES | `TRY_CAST(VendorID AS INT)` |
| `pickup_datetime` | `TIMESTAMP` | YES | Giữ NULL nguồn; giá trị không NULL nhưng parse lỗi vào quarantine |
| `dropoff_datetime` | `TIMESTAMP` | YES | Giữ NULL nguồn; giá trị không NULL nhưng parse lỗi vào quarantine |
| `passenger_count` | `INT` | YES | `TRY_CAST(passenger_count AS INT)`. (NULL cho 4,15M dòng khuyết) |
| `trip_distance` | `NUMERIC(18,6)` | YES | Không làm tròn về 2 chữ số trước phân nhóm; độ chính xác vượt 6 chữ số phải được phát hiện/review, không âm thầm thay đổi |
| `rate_code_id` | `INT` | YES | `TRY_CAST(RatecodeID AS INT)` |
| `store_and_fwd_flag` | `VARCHAR(1)` | YES | Chuẩn hóa `'Y'` hoặc `'N'`. Giá trị khác gán NULL |
| `pu_location_id` | `INT` | YES | Giữ mã nguồn/NULL; chỉ ánh xạ key 0 khi tạo FK |
| `do_location_id` | `INT` | YES | Giữ mã nguồn/NULL; chỉ ánh xạ key 0 khi tạo FK |
| `payment_type` | `INT` | YES | `TRY_CAST(payment_type AS INT)` (0 = Chưa xác định) |
| `fare_amount` | `NUMERIC(18,2)` | YES | Cước tính theo đồng hồ |
| `extra` | `NUMERIC(18,2)` | YES | Phụ phí nguồn |
| `mta_tax` | `NUMERIC(18,2)` | YES | Thuế MTA nguồn |
| `tip_amount` | `NUMERIC(18,2)` | YES | Tiền boa ghi nhận trên thẻ tín dụng |
| `tolls_amount` | `NUMERIC(18,2)` | YES | Phí cầu đường |
| `improvement_surcharge`| `NUMERIC(18,2)` | YES | Phụ phí cải tiến công nghệ |
| `congestion_surcharge` | `NUMERIC(18,2)` | YES | Phụ phí ùn tắc nguồn |
| `airport_fee` | `NUMERIC(18,2)` | YES | Phí sân bay nguồn |
| `total_amount` | `NUMERIC(18,2)` | YES | Tổng tiền nguồn; thiếu thì `is_amount_usable = FALSE` |
| `trip_duration_seconds`| `NUMERIC(20,6)` | YES | `EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))`; đủ rộng và giữ phần giây lẻ |
| `is_time_usable` | `BOOLEAN` | **NO** | Điều kiện thời gian hợp lệ (xem mục 2.2) |
| `is_distance_usable` | `BOOLEAN` | **NO** | Điều kiện quãng đường hợp lệ (xem mục 2.2) |
| `is_amount_usable` | `BOOLEAN` | **NO** | Điều kiện tiền tệ hợp lệ (xem mục 2.2) |
| `has_dq_issue` | `BOOLEAN` | **NO** | `NOT (is_time_usable AND is_distance_usable AND is_amount_usable)` |
| `data_quality_flags` | `TEXT` | YES | Các mã lỗi usable không lặp, nối bằng `;` (vd: `NEGATIVE_FARE;ZERO_DISTANCE`); không lỗi thì NULL |
| `attribute_quality_flags` | `TEXT` | YES | Cảnh báo thiếu/bất thường thuộc tính hoặc chưa ánh xạ; tách khỏi ba cờ usable |
| `quality_rule_version` | `VARCHAR(32)` | **NO** | Phiên bản rule áp dụng đồng nhất trong flagged và Fact |
| `source_file_name` | `VARCHAR(255)` | **NO** | Tên tệp nguồn (vd: `yellow_tripdata_2021-01.parquet`) |
| `source_file_sha256` | `CHAR(64)` | **NO** | Mã hash SHA-256 của tệp nguồn phục vụ chống nạp trùng |
| `source_row_number` | `BIGINT` | **NO** | Số thứ tự dòng thực tế trong tệp nguồn (bắt đầu từ 1) |
| `load_batch_id` | `VARCHAR(64)` | **NO** | UUID phiên nạp do Mage cấp |
| `loaded_at_utc` | `TIMESTAMP` | **NO** | Thời điểm nạp bản ghi vào CSDL theo UTC |

NULL nguồn được giữ nguyên; không âm thầm biến lỗi chuyển kiểu thành NULL. `TRY_CAST` biểu thị chuyển kiểu an toàn; implementation phải tương thích PostgreSQL. Hash tính một lần mỗi file; số dòng liên tục qua các batch/row group. Overflow, lỗi parse và số không hữu hạn vào quarantine, giữ lineage; không cắt phần lẻ để ép thành mã nguyên.

---

### 2.2. Quy tắc Logic kiểm tra Bộ cờ Chất lượng (DQ Usability Logic)

1. **`is_time_usable = TRUE`** khi thỏa mãn đồng thời:
   * Hai timestamp không NULL và ngày thuộc miền **31/12/2020–01/01/2024** (bao gồm hai biên). Ngoài miền: giữ timestamp, DateKey tương ứng về 0, TimeKey vẫn theo giờ/phút nguồn; gắn `DATE_OUT_OF_DOMAIN`, time unusable. Timestamp NULL: DateKey/TimeKey tương ứng về 0, gắn `MISSING_TIME`, time unusable.
   * `dropoff_datetime > pickup_datetime` (thời điểm trả khách sau thời điểm đón khách).
   * `trip_duration_seconds > 0 AND trip_duration_seconds <= 86400` (thời lượng chuyến không vượt quá 24 giờ).
2. **`is_distance_usable = TRUE`** khi thỏa mãn:
   * `trip_distance > 0 AND trip_distance <= 100` (quãng đường lớn hơn 0 và không vượt quá 100 dặm).
   * NULL/âm/0/>100 lần lượt gắn `MISSING_DISTANCE`/`NEGATIVE_DISTANCE`/`ZERO_DISTANCE`/`DISTANCE_OUTLIER`.
3. **`is_amount_usable = TRUE`** khi thỏa mãn đồng thời cả 3 điều kiện tài chính:
   * **Điều kiện 1 (Đủ dữ liệu và không âm):** total/fare/tip không NULL; `total_amount > 0 AND fare_amount >= 0 AND tip_amount >= 0`. Không suy đoán nguyên nhân giao dịch âm.
   * **Điều kiện 2 (Chặn cực trị):** `total_amount <= 5000`.
   * **Điều kiện 3 (Cấu thành cước hợp lý):**  
     `total_amount + 0.01 >= fare_amount + tip_amount + COALESCE(tolls_amount, 0)`. Tolls thiếu chỉ dùng 0 trong phép kiểm, giá trị lưu vẫn NULL; airport/congestion thiếu không tự làm amount unusable.
4. **`has_dq_issue = NOT (is_time_usable AND is_distance_usable AND is_amount_usable)`**. Mọi cờ luôn TRUE/FALSE, không NULL. `HasDQIssue = FALSE` không khẳng định mọi thuộc tính đầy đủ hoặc đã ánh xạ.

**Mã lỗi thống nhất:** thời gian dùng `TIME_REVERSED` cho dropoff ≤ pickup và `DURATION_OVER_24H` cho duration > 24 giờ. Tiền dùng `MISSING_TOTAL_AMOUNT`, `MISSING_FARE_AMOUNT`, `MISSING_TIP_AMOUNT`, `NEGATIVE_TOTAL_AMOUNT`, `ZERO_TOTAL_AMOUNT`, `NEGATIVE_FARE`, `NEGATIVE_TIP`, `AMOUNT_OVER_5000`, `INCONSISTENT_TOTAL` theo từng điều kiện tương ứng. Kiểm độc lập tất cả rule. Cả hai trường flags chứa mã không lặp, sắp alphabet, nối bằng `;`; không có mã thì NULL. Thứ tự không biểu thị mức độ nghiêm trọng.

**Cờ thuộc tính:** khách NULL → `MISSING_PASSENGER_COUNT`, ≤0/>9 → `INVALID_PASSENGER_COUNT`; Vendor NULL/chưa ánh xạ → `MISSING_VENDOR`/`UNMAPPED_VENDOR`; Rate NULL/chưa ánh xạ → `MISSING_RATE_CODE`/`UNMAPPED_RATE_CODE`; store flag NULL → `MISSING_STORE_AND_FWD_FLAG`, có giá trị nhưng khác Y/N sau chuẩn hóa → `INVALID_STORE_AND_FWD_FLAG`. Các cảnh báo này không tự làm amount unusable.

Flagged và dimensions cùng dùng seed mapping đã chốt; flagged không phụ thuộc kết quả bảng dimension để tránh vòng phụ thuộc. Timestamp chuyến giữ giờ địa phương nguồn; vận hành dùng UTC. Chọn file theo tháng/năm manifest, nhóm KPI theo DateKey/timestamp. Tiền chuẩn hóa đến cent, làm tròn nửa ra xa 0, giữ raw và gắn `MONEY_ROUNDED` nếu thay đổi; cự ly chỉ làm tròn để hiển thị, không trước kiểm biên. Ngày ngoài miền không làm loại cả dòng khỏi Fact.

---

## 3. ĐẶC TẢ TẦNG DATA WAREHOUSE (DDS STAR SCHEMA)

### 3.1. Danh mục 8 Bảng Chiều (Dimension Tables)
* **`dds.DimDate`:** Từ **31/12/2020 đến hết 01/01/2024**: 1.097 ngày + Unknown = **1.098 dòng**. `DateKey` dạng `YYYYMMDD`; thiếu/ngoài miền về key 0, giữ timestamp nguồn.
* **`dds.DimTime`:** Đúng **1.441 dòng** (1.440 phút + Unknown `TimeKey = 0`); `TimeKey = hour*60 + minute + 1`.
* **`dds.DimZone`:** **265 mã nghiệp vụ + key 0 = 266 dòng**. `ZoneGeometry` dùng PostGIS `MultiPolygon`, EPSG:4326.  
  * Kỳ vọng 263 zone có polygon hợp lệ; kiểm dữ liệu thực tế, uniqueness và geometry trước khi xác nhận `HasGeometry = True`.
  * `HasGeometry = False` cho key 0, mã 264 (Unknown) và 265 (Outside of NYC).
  * Lookup hiện tại: `Data/taxi+_zone_lookup.csv`; lưu provenance/hash/version, join shapefile bằng LocationID, xác minh CRS trước chuyển EPSG:4326.
* **`dds.DimVendor`:** Danh mục nhà cung cấp thiết bị TPEP (`VendorKey`, `VendorID`, `VendorName`).
* **`dds.DimPaymentType`:** Phương thức thanh toán (`PaymentTypeKey`, `PaymentTypeCode`, `PaymentTypeName`).  
  * Cờ **`IsElectronicTipObservable = True`** duy nhất cho Credit Card (mã 1), các phương thức khác gán `False`.
* **`dds.DimRateCode`:** Biểu phí áp dụng (`RateCodeKey`, `RateCodeID`, `RateCodeName`, `RateGroup`).
* **`dds.DimPassengerGroup`:** 1 → `SOLO`; 2–4 → `GROUP_2_4`; 5–9 → `GROUP_5_PLUS`; NULL/≤0/>9 → `INVALID_OR_MISSING` (key 0). Giữ số khách nguồn; giới hạn 9 là rule dự án có phiên bản.
* **`dds.DimDistanceBand`:** NULL/âm → `INVALID` (key 0); 0 → `ZERO`; (0,2] → `SHORT`; (2,5] → `MEDIUM`; (5,100] → `LONG`; >100 → `OUTLIER`. Không chồng biên.

Vendor/RateCode có `MappingStatus`, `MetadataVersion`; mapping chưa xác minh về key 0 và giữ raw code tại staging. Seed fixture không thay danh mục sản xuất. Payment code nguồn 0 → key 0 bằng mapping tường minh. Dimensions có `MetadataVersion` cho danh mục hoặc `RuleVersion` cho nhóm dẫn xuất; khóa ổn định qua rerun.

**Bảng khóa dùng chung:**

| Dimension | Quy tắc khóa |
| :--- | :--- |
| DimDate | Ngày trong miền: `YYYYMMDD`; thiếu/ngoài miền: 0 |
| DimTime | `hour*60 + minute + 1` (1–1440); timestamp NULL: 0 |
| DimZone | `ZoneKey = LocationID` cho 1–265; thiếu/ngoài danh mục: 0 |
| DimVendor | Mã đã xác minh: `VendorKey = VendorID`; thiếu/chưa xác minh: 0 |
| DimPaymentType | Mã đã xác minh khác 0: key = code; mã nguồn 0, thiếu/chưa ánh xạ: 0 |
| DimRateCode | Mã đã xác minh: `RateCodeKey = RateCodeID`; thiếu/chưa xác minh: 0 |
| DimPassengerGroup | 0 INVALID_OR_MISSING; 1 SOLO; 2 GROUP_2_4; 3 GROUP_5_PLUS |
| DimDistanceBand | 0 INVALID; 1 ZERO; 2 SHORT; 3 MEDIUM; 4 LONG; 5 OUTLIER |

Khóa bằng mã nguồn là quy ước tường minh, không dùng sequence/thứ tự dòng để cấp lại. stinh bàn giao seed gồm raw code, key, tên/nhóm, MappingStatus, nguồn và version; nbinh review để tính cờ, ttu review để ghép Fact. Vendor 5/6 và rate 99 phải xác minh trước cấp khóa sản xuất. Fixture cho phép rate 99 → key 99 như giả định kiểm thử; seed fixture/sản xuất tách riêng và chọn bằng cấu hình.

Mapping thay đổi phải tăng version và xác định phạm vi Fact ánh xạ lại. Giai đoạn này dùng một mapping hiện hành thống nhất; nếu ý nghĩa mã đổi theo thời kỳ, review thiết kế trước áp dụng bảng khóa. PostgreSQL quote thống nhất tên DDS PascalCase, ví dụ `dds."FactTaxiTrip"`, `"PickupDateKey"`; dbt dùng alias/quoting tương ứng. Staging/meta dùng lowercase snake_case.

### 3.2. Bảng Sự kiện trung tâm `dds.FactTaxiTrip`
* **Grain:** Một dòng nguồn chuẩn hóa thành công thuộc phiên bản file đang có hiệu lực; không deduplicate theo giá trị nghiệp vụ.
* **Khóa chính:** `TripKey CHAR(32)` dùng cùng giá trị với `trip_id`; unique thêm theo `(SourceFileSHA256, SourceRowNumber)`.
* **11 Khóa ngoại Role-playing (Bắt buộc NOT NULL, trỏ về 0 nếu thiếu):**
  * `PickupDateKey`, `DropoffDateKey` &rarr; `DimDate(DateKey)`
  * `PickupTimeKey`, `DropoffTimeKey` &rarr; `DimTime(TimeKey)`
  * `PickupZoneKey`, `DropoffZoneKey` &rarr; `DimZone(ZoneKey)`
  * `VendorKey`, `PaymentTypeKey`, `RateCodeKey`, `PassengerGroupKey`, `DistanceBandKey`.
* **Measures:** `TripCount` (=1), `PassengerCount`, `TripDistanceMiles`, `TripDurationSeconds`, `FareAmount`, `Extra`, `MtaTax`, `TipAmount`, `TollsAmount`, `ImprovementSurcharge`, `CongestionSurcharge`, `AirportFee`, `TotalAmount`.
* **Trường Lineage:** `SourceFileName`, `SourceFileSHA256`, `SourceRowNumber`, `LoadBatchID`, `LoadedAtUTC`.
* **Thuộc tính giữ lại:** `PickupDateTime`, `DropoffDateTime`, `StoreAndFwdFlag`.
* **Chất lượng:** `IsTimeUsable`, `IsDistanceUsable`, `IsAmountUsable`, `HasDQIssue`, `DataQualityFlags`, `AttributeQualityFlags`, `QualityRuleVersion`. Kiểu/nullable của measures, timestamps và lineage kế thừa staging; `TripCount` luôn 1, không NULL.
* **Incremental:** ttu quản lý `delete+insert` và DDL. Khi hash đổi, phải thay toàn bộ phần đóng góp cũ theo manifest trước khi công bố bản mới; `delete+insert` theo TripKey mới không tự loại bản cũ. Join không được làm mất hoặc nhân dòng.
* **Ghép và kiểm chứng:** Mỗi mã nguồn ánh xạ tối đa một khóa; Fact/flagged/dimensions dùng cùng seed/version. Kiểm một-một, số dòng và tổng tiền trước/sau join trên cùng phạm vi. Báo riêng tỷ lệ key 0 và raw code liên quan; không có FK mồ côi chưa đủ chứng minh mapping đúng.

---

## 4. TẬP DỮ LIỆU KIỂM THỬ GIẢ LẬP & BẢNG KỲ VỌNG NGHIỆM THU

Tệp hiện tại: **`Data_Contract_and_Mook_Guid.pdf/mock_yellow_trips.csv`** (18 dòng smoke test, gồm TC01 &rarr; TC06). Đường dẫn bàn giao dự kiến: `data/mock/mock_yellow_trips.csv`; chưa khẳng định đã di chuyển file.

### Bảng tra cứu Kết quả Kỳ vọng chi tiết cho 18 dòng mẫu

| Dòng | Kịch bản kiểm thử | `is_time_usable` | `is_distance_usable` | `is_amount_usable` | `has_dq_issue` | Mã lỗi (`data_quality_flags`) | Kỳ vọng ánh xạ Fact &amp; Dimension |
| :---: | :--- | :---: | :---: | :---: | :---: | :--- | :--- |
| **1** | Chuyến chuẩn 1 khách, trả thẻ, tip $3.00. | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | Đủ 11 FK hợp lệ. Thuộc nhóm Solo, cự ly `(2,5]`. `PassengerGroupKey=1`, `DistanceBandKey=3`. |
| **2** | Chuyến chuẩn 3 khách, tiền mặt (tip $0.00). | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | Nhóm `GROUP_2_4`. Tip=0 không ảnh hưởng KPI Tip thẻ. `PassengerGroupKey=2`, `DistanceBandKey=3`. |
| **3** | Chuyến nhóm đông (6 khách), đi sân bay JFK, có phí cầu đường. | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | Nhóm `GROUP_5_PLUS`, biểu phí `JFK` (RateCodeKey=2). `PassengerGroupKey=3`, `DistanceBandKey=4`. |
| **4** | **TC06:** Chuyến vắt qua giao thừa ranh giới năm (31/12/2022 23:50 &rarr; 01/01/2023 00:15). | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | Role-playing Date/Time tách biệt:<br>• `PickupDateKey = 20221231`, `PickupTimeKey = 1431`<br>• `DropoffDateKey = 20230101`, `DropoffTimeKey = 16` `PassengerGroupKey=2`, `DistanceBandKey=4`. |
| **5** | **TC01:** Mẫu khuyết passenger, rate, store_fwd, congestion; payment = 0 (không chứng minh đồng-null trên toàn nguồn). | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | `PassengerGroupKey = 0`, `RateCodeKey = 0`, `PaymentTypeKey = 0`; tiền $15.80 vẫn đủ điều kiện. `attribute_quality_flags`: `MISSING_PASSENGER_COUNT;MISSING_RATE_CODE;MISSING_STORE_AND_FWD_FLAG`. `PassengerGroupKey=0`, `DistanceBandKey=3`. |
| **6** | **TC02a:** Đón khách tại mã khu vực 264 (Unknown). | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | `PickupZoneKey = 264`, DimZone có `HasGeometry = False` (loại khỏi bản đồ đa giác). `PassengerGroupKey=1`, `DistanceBandKey=4`. |
| **7** | **TC02b:** Trả khách tại mã khu vực 265 (Outside of NYC). | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | `DropoffZoneKey = 265`, DimZone có `HasGeometry = False`. `PassengerGroupKey=1`, `DistanceBandKey=4`. |
| **8** | **TC03a:** Thời gian đảo ngược ($Dropoff \le Pickup$, 14:30 &rarr; 14:15). | **FALSE** | **TRUE** | **TRUE** | **TRUE** | `TIME_REVERSED` | Giữ trong Fact để kiểm toán, cờ `is_time_usable = 0` loại khỏi tính vận tốc/thời lượng. `PassengerGroupKey=1`, `DistanceBandKey=2`. |
| **9** | **TC03b:** Chuyến xe kéo dài bất thường trên 24 giờ (26,5 tiếng). | **FALSE** | **TRUE** | **TRUE** | **TRUE** | `DURATION_OVER_24H` | Giữ trong Fact, `is_time_usable = 0`. `PassengerGroupKey=1`, `DistanceBandKey=3`. |
| **10** | **TC04a:** Quãng đường bằng 0 dặm (`trip_distance = 0.00`). | **TRUE** | **FALSE** | **TRUE** | **TRUE** | `ZERO_DISTANCE` | `DistanceBandKey` trỏ về nhóm `ZERO`. Loại khỏi phép tính vận tốc. `PassengerGroupKey=1`, `DistanceBandKey=1`. |
| **11** | **TC04b:** Quãng đường dài bất thường 145 dặm (&gt; 100 miles). | **TRUE** | **FALSE** | **TRUE** | **TRUE** | `DISTANCE_OUTLIER` | `DistanceBandKey` trỏ về nhóm `OUTLIER`. Loại khỏi phép tính vận tốc. `PassengerGroupKey=1`, `DistanceBandKey=5`. |
| **12** | **TC04c:** Quãng đường cực trị lỗi 389.678,46 dặm. | **TRUE** | **FALSE** | **TRUE** | **TRUE** | `DISTANCE_OUTLIER` | Không dùng mã extreme khi chưa có ngưỡng riêng. `PassengerGroupKey=1`, `DistanceBandKey=5`. |
| **13** | **TC05a:** Giao dịch total -$15.30, fare -$12; chưa xác minh nguyên nhân. | **TRUE** | **TRUE** | **FALSE** | **TRUE** | `INCONSISTENT_TOTAL;NEGATIVE_FARE;NEGATIVE_TOTAL_AMOUNT` | Giữ trong Fact; loại khỏi tổng tiền đủ điều kiện. Total + 0.01 vẫn nhỏ hơn fare + tip + tolls. `PassengerGroupKey=1`, `DistanceBandKey=2`. |
| **14** | **TC05b:** Mẫu cực trị năm 2022 (Fare -$133M, Tip +$133M). | **TRUE** | **TRUE** | **FALSE** | **TRUE** | `AMOUNT_OVER_5000;NEGATIVE_FARE` | Kiểu tiền phải chứa được giá trị trước khi kiểm DQ. `PassengerGroupKey=1`, `DistanceBandKey=3`. |
| **15** | Tổng tiền vượt trần 5.000 USD (cước $6.503,30). | **TRUE** | **TRUE** | **FALSE** | **TRUE** | `AMOUNT_OVER_5000` | `is_amount_usable = 0`. `PassengerGroupKey=1`, `DistanceBandKey=4`. |
| **16** | Bất hợp lý cấu thành cước ($Total < Fare + Tip + Tolls$). | **TRUE** | **TRUE** | **FALSE** | **TRUE** | `INCONSISTENT_TOTAL` | `is_amount_usable = 0`. `PassengerGroupKey=1`, `DistanceBandKey=3`. |
| **17** | Biểu phí `RatecodeID = 99`. | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | Fixture ánh xạ `Other / 99`, `RateCodeKey = 99`; cần xác minh trước khi đưa vào seed sản xuất. `PassengerGroupKey=1`, `DistanceBandKey=3`. |
| **18** | Nhà cung cấp `VendorID = 5`. | **TRUE** | **TRUE** | **TRUE** | **FALSE** | `NULL` | Fixture về key 0; `attribute_quality_flags = UNMAPPED_VENDOR`; mapping sản xuất cần xác minh. `PassengerGroupKey=1`, `DistanceBandKey=2`. |

Bàn giao file expected theo source row number gồm ba cờ usable, HasDQIssue, hai trường flags và các khóa cần kiểm, kèm seed fixture có version; review độc lập với implementation.

Bổ sung test biên NULL/0/>9 khách; distance 2/5/100; duration đúng 24 giờ; tiền đúng 5.000 và tolerance; ngày ngoài miền; schema drift; đổi tên/đổi hash; dừng và phục hồi. Expected được review độc lập với implementation. Rerun phải giữ số dòng, tổng tiền và nội dung dữ liệu; bộ 18 dòng không thay kiểm thử đủ 36 file.


| Ca kiểm bổ sung | Expected |
| :--- | :--- |
| Một file mỗi schema nguồn | Cùng schema staging, không mất airport fee |
| Cả hai tên airport fee trong một file | `SCHEMA_ALIAS_CONFLICT`, không nạp âm thầm |
| Passenger/rate 1.0 và 1.5 | 1.0 → 1; 1.5 → quarantine |
| Ngày 31/12/2020 và 01/01/2024 | Có DateKey tương ứng |
| Ngày ngoài hai biên | DateKey=0; giữ timestamp; time unusable |
| Timestamp NULL | DateKey/TimeKey phía tương ứng = 0 |
| Cự ly 1.999999, 2, 2.000001 | SHORT, SHORT, MEDIUM |
| Cự ly 100 và 100.000001 | LONG/usable và OUTLIER/unusable |
| Rerun dimensions | Đúng khóa cố định, không chỉ không mồ côi |

---

## 5. HƯỚNG DẪN PHỐI HỢP PHÁT TRIỂN &amp; KIỂM THỬ NHANH

### 5.1. Dành cho `nbinh` (Staging Transformation &amp; DQ Framework)
* Dùng fixture hiện tại ở Mục 4 để phát triển `stg_yellow_trips.sql`, `stg_trips_flagged.sql`, `dq_rules.sql`, validator và quarantine.
* **B01:** bàn giao mapping hai schema, chuyển kiểu/quarantine, miền ngày và expected fixture; chuẩn hóa chạy được. **B02:** dùng seed chung tính cờ mapping; cờ, mã lỗi khớp bộ fixture và test biên đã thống nhất (không giới hạn ở “10 ca dị thường”). Cả ba review giao diện.
* **B03:** viết SQL sau T03; audit ba năm chờ S03. B04 đối soát count/tiền trên cùng phạm vi.

### 5.2. Dành cho `stinh` (8 Dimensions &amp; Spatial PostGIS)
* **S01:** ingestion 36 Parquet theo batch, lookup/shapefile, hash/số dòng và manifest; kiểm ít nhất một file mỗi schema, ghi schema version. **S02:** xây 8 dimensions theo Mục 3.1, có key 0 và khóa ổn định.
* Với `DimZone`, tích hợp lệnh PostGIS `ST_Transform(geom, 4326)` và gán cờ `HasGeometry = False` cho dòng mẫu số 6 (mã 264) và số 7 (mã 265).
* **Nghiệm thu S02:** cần schema B01, dữ liệu zone S01 và PostGIS T01; models chạy/tests pass, đúng mapping và số dòng (DimDate 1.098; DimTime 1.441; DimZone 266), kiểm geometry thực tế, không FK mồ côi khi tích hợp.

### 5.3. Dành cho `ttu` (FactTaxiTrip Incremental &amp; Master Orchestration)
* Lập trình dbt model `FactTaxiTrip` thực hiện 11 phép JOIN với 8 Dimension theo bộ khóa bàn giao; quản lý phiên bản file hiệu lực và trạng thái batch.
* **T01:** thống nhất PostgreSQL/PostGIS, schema `staging`/`dds`/`meta`, quoting và cách chạy dbt; nghiệm thu môi trường/cấu trúc. **T02:** có thể phát triển bằng fixture; nghiệm thu tích hợp cần B02 và S02:
  * Nạp thử nghiệm 18 dòng mock vào Fact: đủ đúng 18 dòng, không dòng nào bị loại bỏ do join.
  * Nạp lại vẫn 18 dòng, cùng nội dung và tổng tiền; kiểm riêng thay phiên bản file và phục hồi.
* **T04:** kiểm phục hồi sau T03; tổng hợp báo cáo sau B04/S04. Chỉ công bố thành công sau tests và đối soát bắt buộc.

**KPI dùng chung:** công bố riêng tổng tiền nguồn và tổng tiền đủ điều kiện (`IsAmountUsable = TRUE`), chưa gọi là doanh thu thuần/gộp. Tỷ lệ tip = `100 × SUM(TipAmount) / SUM(FareAmount)` trên cùng tập amount usable, thẻ tín dụng và fare > 0. Vận tốc = `3600 × SUM(distance) / SUM(duration)` trên cùng tập time/distance usable. Mẫu số 0 trả NULL; không thay TotalAmount bằng tổng thành phần.

**Điều kiện khóa contract:** Cả ba xác nhận mapping nguồn, miền ngày, bảng khóa, seed fixture và expected trước nghiệm thu B01. Kế hoạch dẫn đúng phiên bản contract; giờ công/deadline tiếp tục quản lý trong kế hoạch.
