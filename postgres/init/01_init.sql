-- ============================================================================
-- DỰ ÁN KHO DỮ LIỆU & BI NYC YELLOW TAXI (2021 - 2023) - NHÓM 09
-- TÀI LIỆU DDL KHỞI TẠO HẠ TẦNG CSDL & SINGLE DDS STAR SCHEMA
-- TASK: T01 | CHỦ TRÌ: TTU (PHẠM LÊ THANH TÚ - NHÓM TRƯỞNG)
-- PHIÊN BẢN: v1.2 (CHUẨN HÓA THEO DATA CONTRACT VÀ BÁO CÁO MODELING)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. KÍCH HOẠT EXTENSION POSTGIS (PHỤC VỤ PHÂN TÍCH KHÔNG GIAN ĐỊA LÝ)
-- ----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis;

-- ----------------------------------------------------------------------------
-- 2. TẠO 3 SCHEMA CHÍNH THỨC CỦA HỆ THỐNG SINGLE DDS
-- ----------------------------------------------------------------------------
-- staging: Vùng đệm kỹ thuật, tiếp nhận và chuẩn hóa dữ liệu thô
CREATE SCHEMA IF NOT EXISTS staging;

-- dds: Kho dữ liệu đa chiều trung tâm (Single DDS Star Schema)
CREATE SCHEMA IF NOT EXISTS dds;

-- meta: Quản lý siêu dữ liệu, manifest file chống nạp trùng và nhật ký batch
CREATE SCHEMA IF NOT EXISTS meta;

-- ============================================================================
-- 3. SCHEMA META: BẢNG MANIFEST VÀ NHẬT KÝ VẬN HÀNH (METADATA & AUDIT)
-- ============================================================================

-- 3.1. Bảng Manifest quản lý trạng thái 36 tệp Parquet
-- Hỗ trợ STINH (Task S01) kiểm tra chống nạp trùng dựa trên SHA-256
CREATE TABLE IF NOT EXISTS meta.file_manifest (
    file_id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL UNIQUE,              -- Tên tệp (vd: yellow_tripdata_2021-01.parquet)
    file_path VARCHAR(500) NOT NULL,                     -- Đường dẫn tệp nguồn
    file_sha256 CHAR(64) NOT NULL UNIQUE,                -- Mã băm SHA-256 nội dung tệp
    file_size_bytes BIGINT,                              -- Dung lượng tệp (bytes)
    total_rows BIGINT,                                   -- Tổng số dòng thực tế từ footer file gốc
    source_schema_version VARCHAR(32) NOT NULL,          -- 'v1_2021_2022' hoặc 'v2_2023' (phát hiện schema drift)
    load_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',  -- PENDING, PROCESSING, SUCCESS, FAILED
    current_batch_id VARCHAR(64),                        -- UUID phiên nạp do Mage cấp
    staged_rows BIGINT DEFAULT 0,                        -- Số dòng nạp thành công vào staging
    fact_rows BIGINT DEFAULT 0,                          -- Số dòng nạp thành công vào Fact
    quarantine_rows BIGINT DEFAULT 0,                    -- Số dòng đẩy vào bảng quarantine
    first_loaded_at TIMESTAMP WITH TIME ZONE,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

-- 3.2. Bảng Batch Log: Ghi nhận vết thực thi từng lượt chạy của Mage Orchestrator
-- Hỗ trợ TTU (Task T03) và NBINH (Task B04) đối soát vận hành
CREATE TABLE IF NOT EXISTS meta.batch_log (
    batch_id VARCHAR(64) PRIMARY KEY,                   -- UUID phiên nạp do Mage cấp
    pipeline_name VARCHAR(100) NOT NULL,                -- Tên pipeline (ingest_raw_trips, master_etl_pipeline)
    batch_status VARCHAR(32) NOT NULL DEFAULT 'STARTED',-- STARTED, COMPLETED, COMPLETED_WITH_QUARANTINE, FAILED
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10, 2),
    processed_records BIGINT DEFAULT 0,
    error_message TEXT
);

-- ============================================================================
-- 4. SCHEMA STAGING: KHUNG BẢNG RAW VÀ BẢNG QUARANTINE
-- ============================================================================

-- 4.1. Bảng tiếp nhận dữ liệu thô: staging.raw_yellow_trips
-- Cung cấp cấu trúc chuẩn xác để STINH (Task S01) viết PyArrow Chunk Loader nạp vào
CREATE TABLE IF NOT EXISTS staging.raw_yellow_trips (
    -- Metadata truy vết nguồn gốc (Data Lineage)
    source_file_name VARCHAR(255) NOT NULL,
    source_file_sha256 CHAR(64) NOT NULL,
    source_row_number BIGINT NOT NULL,
    load_batch_id VARCHAR(64) NOT NULL,
    loaded_at_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),

    -- 19 trường dữ liệu giao dịch chuyến đi từ tệp nguồn Parquet
    vendor_id VARCHAR(50),
    tpep_pickup_datetime VARCHAR(50),
    tpep_dropoff_datetime VARCHAR(50),
    passenger_count VARCHAR(50),
    trip_distance VARCHAR(50),
    rate_code_id VARCHAR(50),
    store_and_fwd_flag VARCHAR(50),
    pu_location_id VARCHAR(50),
    do_location_id VARCHAR(50),
    payment_type VARCHAR(50),
    fare_amount VARCHAR(50),
    extra VARCHAR(50),
    mta_tax VARCHAR(50),
    tip_amount VARCHAR(50),
    tolls_amount VARCHAR(50),
    improvement_surcharge VARCHAR(50),
    total_amount VARCHAR(50),
    congestion_surcharge VARCHAR(50),
    airport_fee VARCHAR(50)
);

-- 4.2. Bảng Quarantine: Cách ly các bản ghi lỗi chuyển kiểu kỹ thuật
-- Hỗ trợ NBINH (Task B01, B02) hứng các lỗi không thể cast kiểu
CREATE TABLE IF NOT EXISTS staging.quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    source_file_name VARCHAR(255) NOT NULL,
    source_file_sha256 CHAR(64) NOT NULL,
    source_row_number BIGINT NOT NULL,
    load_batch_id VARCHAR(64) NOT NULL,
    quarantine_reason VARCHAR(100) NOT NULL, -- TYPE_CAST_ERROR, OVERFLOW_ERROR, UNPARSABLE_TIMESTAMP
    raw_payload JSONB NOT NULL,              -- Toàn bộ giá trị dòng gốc dạng JSON
    quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 5. SCHEMA DDS: KHỞI TẠO 8 BẢNG CHIỀU (CONFORMED DIMENSIONS)
-- Đầy đủ 100% thuộc tính theo Báo cáo Modeling Lần 1 và Data Contract v1.2
-- ============================================================================

-- 5.1. Bảng chiều Ngày: dds."DimDate" (Đủ 9 thuộc tính)
-- Phục vụ phân tích thời gian theo phân cấp Năm -> Tháng -> Ngày; Role-playing đón/trả
CREATE TABLE IF NOT EXISTS dds."DimDate" (
    "DateKey" INT PRIMARY KEY,               -- Khóa chính: YYYYMMDD (0: Unknown/Missing)
    "FullDate" DATE,                         -- Ngày lịch thực tế (YYYY-MM-DD)
    "YearNumber" INT,                        -- Năm lịch (vd: 2021, 2022, 2023)
    "YearMonth" VARCHAR(7),                  -- Nhãn Năm-Tháng (vd: '2023-05')
    "MonthNumber" INT,                       -- Tháng trong năm (1 - 12)
    "MonthName" VARCHAR(20),                 -- Tên tháng tiếng Anh (vd: 'January')
    "DayOfWeek" INT,                         -- Thứ theo chuẩn ISO (1: Thứ Hai -> 7: Chủ Nhật)
    "DayName" VARCHAR(20),                   -- Tên thứ tiếng Anh (vd: 'Monday')
    "IsWeekend" BOOLEAN                      -- Cờ đánh dấu ngày cuối tuần (Thứ 7, CN)
);

-- 5.2. Bảng chiều Thời gian: dds."DimTime" (Đủ 4 thuộc tính, 1.441 dòng)
-- Tách biệt khỏi DimDate để tránh bùng nổ số dòng; Role-playing đón/trả
CREATE TABLE IF NOT EXISTS dds."DimTime" (
    "TimeKey" INT PRIMARY KEY,               -- Khóa chính: 1 - 1440 (0: Unknown/Missing)
    "TimeLabel" VARCHAR(5),                  -- Nhãn hiển thị HH:mm (vd: '08:30')
    "HourNumber" INT,                        -- Giờ trong ngày (0 - 23)
    "TimeBand" VARCHAR(10)                   -- Phân nhóm khung giờ: '00-05', '06-11', '12-17', '18-23'
);

-- 5.3. Bảng chiều Khu vực: dds."DimZone" (Đủ 9 thuộc tính)
-- Tích hợp không gian địa lý PostGIS (EPSG:4326), Role-playing đón/trả
CREATE TABLE IF NOT EXISTS dds."DimZone" (
    "ZoneKey" INT PRIMARY KEY,               -- Khóa chính do kho cấp (0: Unknown/Missing)
    "LocationID" INT,                        -- Mã vị trí nghiệp vụ TLC LocationID (1 - 265)
    "ZoneName" VARCHAR(100),                 -- Tên khu vực taxi (vd: 'Times Sq/Theatre District')
    "BoroughName" VARCHAR(50),               -- Tên quận (Manhattan, Queens, Brooklyn, Bronx, Staten Island, EWR)
    "ServiceZone" VARCHAR(50),               -- Phân vùng dịch vụ (Yellow Zone, Boro Zone, Airports)
    "ZoneGeometry" geometry(MultiPolygon, 4326), -- Hình học đa giác ranh giới chuẩn WGS84
    "HasGeometry" BOOLEAN NOT NULL DEFAULT FALSE,-- Cờ xác định có bản đồ đa giác (False cho mã 264, 265)
    "MappingStatus" VARCHAR(20) DEFAULT 'VERIFIED', -- VERIFIED, UNMAPPED, MISSING
    "MetadataVersion" VARCHAR(32) DEFAULT 'v1.0'
);

-- 5.4. Bảng chiều Nhà cung cấp thiết bị: dds."DimVendor" (Đủ 5 thuộc tính)
CREATE TABLE IF NOT EXISTS dds."DimVendor" (
    "VendorKey" INT PRIMARY KEY,             -- Khóa chính do kho cấp (0: Unknown/Missing)
    "VendorID" INT,                          -- Mã nhà cung cấp TPEP từ nguồn (1, 2, ...)
    "VendorName" VARCHAR(100),               -- Tên nhà cung cấp (1: Creative Mobile Tech, 2: VeriFone)
    "MappingStatus" VARCHAR(20) DEFAULT 'VERIFIED',
    "MetadataVersion" VARCHAR(32) DEFAULT 'v1.0'
);

-- 5.5. Bảng chiều Phương thức thanh toán: dds."DimPaymentType" (Đủ 6 thuộc tính)
CREATE TABLE IF NOT EXISTS dds."DimPaymentType" (
    "PaymentTypeKey" INT PRIMARY KEY,        -- Khóa chính do kho cấp (0: Unknown/Missing)
    "PaymentTypeCode" INT,                   -- Mã thanh toán nguồn từ TLC (0, 1, 2, 3, 4, 5, 6)
    "PaymentTypeName" VARCHAR(50),           -- Tên phương thức (Credit card, Cash, No charge, Dispute, ...)
    "IsElectronicTipObservable" BOOLEAN NOT NULL DEFAULT FALSE, -- Cờ ghi nhận tiền tip điện tử (True duy nhất cho Credit Card)
    "MappingStatus" VARCHAR(20) DEFAULT 'VERIFIED',
    "MetadataVersion" VARCHAR(32) DEFAULT 'v1.0'
);

-- 5.6. Bảng chiều Biểu phí cước: dds."DimRateCode" (Đủ 6 thuộc tính)
CREATE TABLE IF NOT EXISTS dds."DimRateCode" (
    "RateCodeKey" INT PRIMARY KEY,           -- Khóa chính do kho cấp (0: Unknown/Missing)
    "RateCodeID" INT,                        -- Mã biểu phí từ nguồn (1 - 6, 99)
    "RateCodeName" VARCHAR(100),             -- Tên loại cước (Standard rate, JFK, Newark, Negotiated, ...)
    "RateGroup" VARCHAR(50),                 -- Nhóm biểu phí: STANDARD, AIRPORT, NEGOTIATED, OTHER
    "MappingStatus" VARCHAR(20) DEFAULT 'VERIFIED',
    "MetadataVersion" VARCHAR(32) DEFAULT 'v1.0'
);

-- 5.7. Bảng chiều Nhóm hành khách: dds."DimPassengerGroup" (Đủ 7 thuộc tính)
-- Banding Dimension phân nhóm khách đi 1 mình, 2-4 người, 5+ người
CREATE TABLE IF NOT EXISTS dds."DimPassengerGroup" (
    "PassengerGroupKey" INT PRIMARY KEY,     -- Khóa chính do kho cấp (0: Invalid / Missing)
    "PassengerGroupCode" VARCHAR(30),        -- Mã phân nhóm: SOLO, GROUP_2_4, GROUP_5_PLUS, INVALID_OR_MISSING
    "PassengerGroupName" VARCHAR(50),        -- Tên hiển thị: '1 khách', '2-4 khách', '5+ khách', 'Bất thường'
    "MinPassengerCount" INT,                 -- Cận dưới số khách (bao gồm: 1, 2, 5)
    "MaxPassengerCount" INT,                 -- Cận trên số khách (bao gồm: 1, 4; NULL cho nhóm 5+)
    "SortOrder" INT,                         -- Thứ tự sắp xếp hiển thị biểu đồ (1, 2, 3, 4)
    "RuleVersion" VARCHAR(32) DEFAULT 'v1.0' -- Phiên bản quy tắc phân nhóm
);

-- 5.8. Bảng chiều Dải cự ly di chuyển: dds."DimDistanceBand" (Đủ 8 thuộc tính)
-- Banding Dimension phân chia dải cự ly không chồng lấn theo quy tắc (a, b]
CREATE TABLE IF NOT EXISTS dds."DimDistanceBand" (
    "DistanceBandKey" INT PRIMARY KEY,       -- Khóa chính do kho cấp (0: Invalid / Âm)
    "DistanceBandCode" VARCHAR(30),          -- Mã cự ly: ZERO, SHORT, MEDIUM, LONG, OUTLIER, INVALID
    "DistanceBandName" VARCHAR(50),          -- Tên hiển thị: '0 miles', '(0-2] miles', '(2-5] miles', '(5-100] miles', '>100 miles'
    "LowerBoundMiles" NUMERIC(10, 2),        -- Cận dưới dải khoảng cách (không bao gồm đối với dải dương)
    "UpperBoundMiles" NUMERIC(10, 2),        -- Cận trên dải khoảng cách (bao gồm; NULL cho >100 dặm)
    "Unit" VARCHAR(10) DEFAULT 'miles',      -- Đơn vị cự ly ('miles')
    "SortOrder" INT,                         -- Thứ tự sắp xếp dải hiển thị (1, 2, 3, 4, 5, 6)
    "RuleVersion" VARCHAR(32) DEFAULT 'v1.0' -- Phiên bản quy tắc phân dải
);

-- ============================================================================
-- 6. NẠP DÒNG MẶC ĐỊNH SURROGATE KEY = 0 (UNKNOWN / MISSING / UNMAPPED)
-- Đảm bảo toàn vẹn tham chiếu 100%, không bị lỗi Foreign Key mồ côi
-- ============================================================================

INSERT INTO dds."DimDate" ("DateKey", "FullDate", "YearNumber", "YearMonth", "MonthNumber", "MonthName", "DayOfWeek", "DayName", "IsWeekend")
VALUES (0, '1900-01-01', 1900, '1900-00', 0, 'Unknown', 0, 'Unknown', FALSE)
ON CONFLICT ("DateKey") DO NOTHING;

INSERT INTO dds."DimTime" ("TimeKey", "TimeLabel", "HourNumber", "TimeBand")
VALUES (0, '00:00', -1, 'Unknown')
ON CONFLICT ("TimeKey") DO NOTHING;

INSERT INTO dds."DimZone" ("ZoneKey", "LocationID", "ZoneName", "BoroughName", "ServiceZone", "ZoneGeometry", "HasGeometry", "MappingStatus", "MetadataVersion")
VALUES (0, 0, 'Unknown Zone', 'Unknown', 'Unknown', NULL, FALSE, 'MISSING', 'v1.0')
ON CONFLICT ("ZoneKey") DO NOTHING;

INSERT INTO dds."DimVendor" ("VendorKey", "VendorID", "VendorName", "MappingStatus", "MetadataVersion")
VALUES (0, 0, 'Unknown / Unmapped Vendor', 'MISSING', 'v1.0')
ON CONFLICT ("VendorKey") DO NOTHING;

INSERT INTO dds."DimPaymentType" ("PaymentTypeKey", "PaymentTypeCode", "PaymentTypeName", "IsElectronicTipObservable", "MappingStatus", "MetadataVersion")
VALUES (0, 0, 'Unknown / Missing Payment', FALSE, 'MISSING', 'v1.0')
ON CONFLICT ("PaymentTypeKey") DO NOTHING;

INSERT INTO dds."DimRateCode" ("RateCodeKey", "RateCodeID", "RateCodeName", "RateGroup", "MappingStatus", "MetadataVersion")
VALUES (0, 0, 'Unknown / Unmapped Rate', 'OTHER', 'MISSING', 'v1.0')
ON CONFLICT ("RateCodeKey") DO NOTHING;

INSERT INTO dds."DimPassengerGroup" ("PassengerGroupKey", "PassengerGroupCode", "PassengerGroupName", "MinPassengerCount", "MaxPassengerCount", "SortOrder", "RuleVersion")
VALUES (0, 'INVALID_OR_MISSING', 'Bất thường / Khuyết', NULL, NULL, 4, 'v1.0')
ON CONFLICT ("PassengerGroupKey") DO NOTHING;

INSERT INTO dds."DimDistanceBand" ("DistanceBandKey", "DistanceBandCode", "DistanceBandName", "LowerBoundMiles", "UpperBoundMiles", "Unit", "SortOrder", "RuleVersion")
VALUES (0, 'INVALID', 'Bất thường / Âm', NULL, NULL, 'miles', 6, 'v1.0')
ON CONFLICT ("DistanceBandKey") DO NOTHING;

-- ============================================================================
-- 7. SCHEMA DDS: BẢNG SỰ KIỆN TRUNG TÂM `dds."FactTaxiTrip"` (SINGLE DDS)
-- Đầy đủ 11 Khóa ngoại, 13 Measures, 7 trường DQ, 5 trường Lineage
-- ============================================================================

CREATE TABLE IF NOT EXISTS dds."FactTaxiTrip" (
    -- 7.1. Khóa chính bất biến (Primary Key)
    -- Sinh từ: MD5(source_file_sha256 || '_' || source_row_number)
    "TripKey" CHAR(32) PRIMARY KEY,

    -- 7.2. Hệ thống 11 Khóa ngoại Role-playing (Bắt buộc NOT NULL, Mặc định = 0)
    "PickupDateKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimDate"("DateKey"),
    "DropoffDateKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimDate"("DateKey"),
    "PickupTimeKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimTime"("TimeKey"),
    "DropoffTimeKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimTime"("TimeKey"),
    "PickupZoneKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimZone"("ZoneKey"),
    "DropoffZoneKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimZone"("ZoneKey"),
    "VendorKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimVendor"("VendorKey"),
    "PaymentTypeKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimPaymentType"("PaymentTypeKey"),
    "RateCodeKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimRateCode"("RateCodeKey"),
    "PassengerGroupKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimPassengerGroup"("PassengerGroupKey"),
    "DistanceBandKey" INT NOT NULL DEFAULT 0 REFERENCES dds."DimDistanceBand"("DistanceBandKey"),

    -- 7.3. Thuộc tính nguồn giữ lại phục vụ kiểm toán và tính toán vi mô
    "PickupDateTime" TIMESTAMP WITHOUT TIME ZONE,
    "DropoffDateTime" TIMESTAMP WITHOUT TIME ZONE,
    "StoreAndFwdFlag" VARCHAR(1),

    -- 7.4. Hệ thống 13 Thước đo định lượng (Base Measures)
    "TripCount" INT NOT NULL DEFAULT 1,             -- Thước đo Additive: Đếm sản lượng chuyến
    "PassengerCount" INT,                           -- Thước đo Additive có điều kiện: Số lượng khách
    "TripDistanceMiles" NUMERIC(18, 6),             -- Quãng đường di chuyển (dặm)
    "TripDurationSeconds" NUMERIC(20, 6),           -- Thời lượng chuyến đi (giây lẻ)
    "FareAmount" NUMERIC(18, 2),                    -- Cước đồng hồ (USD)
    "Extra" NUMERIC(18, 2),                         -- Phụ phí ngoài giờ / cao điểm (USD)
    "MtaTax" NUMERIC(18, 2),                        -- Thuế vận tải công cộng MTA (USD)
    "TipAmount" NUMERIC(18, 2),                     -- Tiền boa ghi nhận qua thẻ tín dụng (USD)
    "TollsAmount" NUMERIC(18, 2),                   -- Phí cầu đường (USD)
    "ImprovementSurcharge" NUMERIC(18, 2),          -- Phụ phí cải tiến công nghệ (USD)
    "CongestionSurcharge" NUMERIC(18, 2),           -- Phụ phí ùn tắc Manhattan (USD)
    "AirportFee" NUMERIC(18, 2),                    -- Phí đón tại sân bay (JFK, LGA) (USD)
    "TotalAmount" NUMERIC(18, 2),                   -- Tổng số tiền giao dịch ghi nhận (USD)

    -- 7.5. Khung cờ chất lượng dữ liệu (Data Quality Framework - Triết lý Preserve & Flag)
    "IsTimeUsable" BOOLEAN NOT NULL DEFAULT FALSE,       -- Cờ hợp lệ thời gian (Dropoff > Pickup, <= 24h, trong miền ngày)
    "IsDistanceUsable" BOOLEAN NOT NULL DEFAULT FALSE,   -- Cờ hợp lệ cự ly (0 < Distance <= 100 miles)
    "IsAmountUsable" BOOLEAN NOT NULL DEFAULT FALSE,     -- Cờ hợp lệ tài chính (Total > 0, Fare >= 0, <= 5000$, đúng cấu thành)
    "HasDQIssue" BOOLEAN NOT NULL DEFAULT TRUE,          -- Cờ tổng hợp lỗi: NOT (IsTimeUsable AND IsDistanceUsable AND IsAmountUsable)
    "DataQualityFlags" TEXT,                             -- Chuỗi mã lỗi usable không lặp (vd: 'NEGATIVE_FARE;ZERO_DISTANCE')
    "AttributeQualityFlags" TEXT,                        -- Cảnh báo thuộc tính khuyết (vd: 'MISSING_PASSENGER_COUNT')
    "QualityRuleVersion" VARCHAR(32) NOT NULL DEFAULT 'v1.2', -- Phiên bản rule chất lượng áp dụng

    -- 7.6. Thuộc tính truy vết nguồn gốc (Data Lineage & Chống nạp trùng)
    "SourceFileName" VARCHAR(255) NOT NULL,              -- Tên tệp nguồn
    "SourceFileSHA256" CHAR(64) NOT NULL,               -- Mã băm nội dung tệp nguồn
    "SourceRowNumber" BIGINT NOT NULL,                   -- Số thứ tự dòng thực tế trong tệp nguồn
    "LoadBatchID" VARCHAR(64) NOT NULL,                  -- UUID phiên nạp do Mage cấp
    "LoadedAtUTC" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),

    -- Ràng buộc duy nhất theo nguồn gốc để ngăn chặn trùng lặp bản ghi
    CONSTRAINT uq_fact_source_lineage UNIQUE ("SourceFileSHA256", "SourceRowNumber")
);

-- ============================================================================
-- 8. HỆ THỐNG CHỈ MỤC TỐI ƯU HÓA HIỆU NĂNG (INDEXING STRATEGY)
-- ============================================================================

-- 8.1. B-Tree Indexes trên toàn bộ 11 Khóa ngoại Fact (Tăng tốc độ JOIN tức thì)
CREATE INDEX IF NOT EXISTS idx_fact_pickup_date ON dds."FactTaxiTrip" ("PickupDateKey");
CREATE INDEX IF NOT EXISTS idx_fact_dropoff_date ON dds."FactTaxiTrip" ("DropoffDateKey");
CREATE INDEX IF NOT EXISTS idx_fact_pickup_time ON dds."FactTaxiTrip" ("PickupTimeKey");
CREATE INDEX IF NOT EXISTS idx_fact_dropoff_time ON dds."FactTaxiTrip" ("DropoffTimeKey");
CREATE INDEX IF NOT EXISTS idx_fact_pickup_zone ON dds."FactTaxiTrip" ("PickupZoneKey");
CREATE INDEX IF NOT EXISTS idx_fact_dropoff_zone ON dds."FactTaxiTrip" ("DropoffZoneKey");
CREATE INDEX IF NOT EXISTS idx_fact_vendor ON dds."FactTaxiTrip" ("VendorKey");
CREATE INDEX IF NOT EXISTS idx_fact_payment ON dds."FactTaxiTrip" ("PaymentTypeKey");
CREATE INDEX IF NOT EXISTS idx_fact_ratecode ON dds."FactTaxiTrip" ("RateCodeKey");
CREATE INDEX IF NOT EXISTS idx_fact_passenger_grp ON dds."FactTaxiTrip" ("PassengerGroupKey");
CREATE INDEX IF NOT EXISTS idx_fact_distance_band ON dds."FactTaxiTrip" ("DistanceBandKey");

-- 8.2. Composite Index trên bộ 3 cờ Usability (Tăng tốc độ truy vấn lọc dữ liệu sạch khi tính KPIs)
CREATE INDEX IF NOT EXISTS idx_fact_usable_flags ON dds."FactTaxiTrip" ("IsAmountUsable", "IsTimeUsable", "IsDistanceUsable");

-- 8.3. GiST Spatial Index trên DimZone (Tối ưu hóa các phép toán không gian PostGIS)
CREATE INDEX IF NOT EXISTS idx_dimzone_geometry ON dds."DimZone" USING GIST ("ZoneGeometry");
