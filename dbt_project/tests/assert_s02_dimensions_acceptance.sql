-- Kiểm tra toàn bộ tiêu chí nghiệm thu của Task S02 (Trả về 0 dòng = PASS 100%)
WITH checks AS (
    -- 1. DimDate phải có đúng 1.098 dòng (1.097 ngày + Key 0)
    SELECT 'DimDate_row_count' AS check_name, COUNT(*) AS actual_val, 1098 AS expected_val
    FROM {{ ref('DimDate') }}
    UNION ALL
    -- 2. DimTime phải có đúng 1.441 dòng (1.440 phút + Key 0)
    SELECT 'DimTime_row_count', COUNT(*), 1441
    FROM {{ ref('DimTime') }}
    UNION ALL
    -- 3. DimZone phải có đúng 266 dòng (265 LocationID + Key 0)
    SELECT 'DimZone_row_count', COUNT(*), 266
    FROM {{ ref('DimZone') }}
    UNION ALL
    -- 4. DimZone phải có 263 polygon hợp lệ (HasGeometry = TRUE)
    SELECT 'DimZone_valid_polygons', COUNT(*), 263
    FROM {{ ref('DimZone') }}
    WHERE "HasGeometry" = TRUE AND "ZoneGeometry" IS NOT NULL AND ST_IsValid("ZoneGeometry") AND ST_SRID("ZoneGeometry") = 4326
    UNION ALL
    -- 5. Mã 0, 264, 265 bắt buộc có HasGeometry = FALSE
    SELECT 'DimZone_non_spatial_codes', COUNT(*), 3
    FROM {{ ref('DimZone') }}
    WHERE "ZoneKey" IN (0, 264, 265) AND "HasGeometry" = FALSE AND "ZoneGeometry" IS NULL
    UNION ALL
    -- 6. Cả 8 Dimension đều phải có thành viên mặc định Key = 0
    SELECT 'All_8_Dims_have_Key_0', (
        (SELECT COUNT(*) FROM {{ ref('DimDate') }} WHERE "DateKey" = 0) +
        (SELECT COUNT(*) FROM {{ ref('DimTime') }} WHERE "TimeKey" = 0) +
        (SELECT COUNT(*) FROM {{ ref('DimZone') }} WHERE "ZoneKey" = 0) +
        (SELECT COUNT(*) FROM {{ ref('DimVendor') }} WHERE "VendorKey" = 0) +
        (SELECT COUNT(*) FROM {{ ref('DimPaymentType') }} WHERE "PaymentTypeKey" = 0) +
        (SELECT COUNT(*) FROM {{ ref('DimRateCode') }} WHERE "RateCodeKey" = 0) +
        (SELECT COUNT(*) FROM {{ ref('DimPassengerGroup') }} WHERE "PassengerGroupKey" = 0) +
        (SELECT COUNT(*) FROM {{ ref('DimDistanceBand') }} WHERE "DistanceBandKey" = 0)
    ), 8
)
SELECT *
FROM checks
WHERE actual_val <> expected_val