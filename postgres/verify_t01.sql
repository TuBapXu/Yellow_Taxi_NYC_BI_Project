SELECT '1. PostGIS Version' AS kiem_tra, postgis_version() AS ket_qua
UNION ALL
SELECT '2. So luong Schemas (staging, dds, meta)', COUNT(*)::text 
FROM information_schema.schemata WHERE schema_name IN ('staging', 'dds', 'meta')
UNION ALL
SELECT '3. Bang FactTaxiTrip ton tai', COUNT(*)::text 
FROM information_schema.tables WHERE table_schema = 'dds' AND table_name = 'FactTaxiTrip'
UNION ALL
SELECT '4. So luong Foreign Keys tren Fact', COUNT(*)::text 
FROM information_schema.table_constraints WHERE table_schema = 'dds' AND table_name = 'FactTaxiTrip' AND constraint_type = 'FOREIGN KEY'
UNION ALL
SELECT '5. So luong Cot cua FactTaxiTrip (ky vong: 37)', COUNT(*)::text 
FROM information_schema.columns WHERE table_schema = 'dds' AND table_name = 'FactTaxiTrip'
UNION ALL
SELECT '6. So luong Dimension co dong Key = 0', (
    (SELECT COUNT(*) FROM dds."DimDate" WHERE "DateKey" = 0) +
    (SELECT COUNT(*) FROM dds."DimTime" WHERE "TimeKey" = 0) +
    (SELECT COUNT(*) FROM dds."DimZone" WHERE "ZoneKey" = 0) +
    (SELECT COUNT(*) FROM dds."DimVendor" WHERE "VendorKey" = 0) +
    (SELECT COUNT(*) FROM dds."DimPaymentType" WHERE "PaymentTypeKey" = 0) +
    (SELECT COUNT(*) FROM dds."DimRateCode" WHERE "RateCodeKey" = 0) +
    (SELECT COUNT(*) FROM dds."DimPassengerGroup" WHERE "PassengerGroupKey" = 0) +
    (SELECT COUNT(*) FROM dds."DimDistanceBand" WHERE "DistanceBandKey" = 0)
)::text
UNION ALL
SELECT '7. Bang Metadata & Quarantine san sang', COUNT(*)::text 
FROM information_schema.tables 
WHERE (table_schema = 'meta' AND table_name IN ('file_manifest', 'batch_log'))
   OR (table_schema = 'staging' AND table_name IN ('raw_yellow_trips', 'quarantine'));
