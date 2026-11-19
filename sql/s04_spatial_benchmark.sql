-- ==============================================================================
-- TASK S04: TỐI ƯU HÓA KHÔNG GIAN POSTGIS & BENCHMARK TRUY VẤN SPATIAL
-- Nhóm 09 - BI Project: Phân tích lộ trình NYC Yellow Taxi
-- Thành viên phụ trách: STINH (Sơn Tịnh) | Reviewer: TTU (Nhóm trưởng)
-- ==============================================================================

-- 1. KIỂM TRA ĐỊNH NGHĨA CHỈ MỤC KHÔNG GIAN GIST TRÊN BẢNG DIMZONE
SELECT 
    schemaname, 
    tablename, 
    indexname, 
    indexdef 
FROM pg_indexes 
WHERE schemaname = 'dds' 
  AND tablename = 'DimZone' 
  AND indexdef LIKE '%gist%';

-- 2. BENCHMARK TRUY VẤN POINT-IN-POLYGON (ĐIỂM TRONG ĐA GIÁC)
-- Yêu cầu đề bài DoD: Thời gian phản hồi < 1.0s
-- Kịch bản 1: Xác định Zone từ tọa độ GPS Sân bay JFK (-73.7781, 40.6413)
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    "ZoneKey",
    "LocationID",
    "ZoneName",
    "BoroughName",
    "ServiceZone",
    ST_AsText(ST_Centroid("ZoneGeometry")) AS Centroid_WKT
FROM dds."DimZone"
WHERE "ZoneGeometry" IS NOT NULL
  AND ST_Contains(
      "ZoneGeometry", 
      ST_SetSRID(ST_Point(-73.7781, 40.6413), 4326)
  );

-- Kịch bản 2: Xác định Zone từ tọa độ GPS Times Square / Manhattan (-73.9855, 40.7580)
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    "ZoneKey",
    "LocationID",
    "ZoneName",
    "BoroughName",
    "ServiceZone"
FROM dds."DimZone"
WHERE "ZoneGeometry" IS NOT NULL
  AND ST_Contains(
      "ZoneGeometry", 
      ST_SetSRID(ST_Point(-73.9855, 40.7580), 4326)
  );

-- 3. BENCHMARK TRUY VẤN HỘP GIỚI HẠN KHÔNG GIAN (BOUNDING BOX && OPERATOR)
-- Lọc các Zone giao cắt với Bounding Box khu vực Trung tâm Manhattan
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    "ZoneKey",
    "ZoneName",
    "BoroughName"
FROM dds."DimZone"
WHERE "ZoneGeometry" && ST_MakeEnvelope(-74.02, 40.70, -73.95, 40.78, 4326);

-- 4. TÍNH DIỆN TÍCH VÀ TỌA ĐỘ TRỌNG TÂM CỦA CÁC ZONE (SPATIAL METRICS)
-- Chuyển đổi sang hệ tọa độ New York State Plane EPSG:2263 (đơn vị: feet) để tính diện tích chuẩn
SELECT 
    "ZoneKey",
    "ZoneName",
    "BoroughName",
    ROUND((ST_Area(ST_Transform("ZoneGeometry", 2263)) * 0.092903 / 1000000)::numeric, 4) AS "Area_Km2",
    ROUND(ST_X(ST_Centroid("ZoneGeometry"))::numeric, 6) AS "Centroid_Longitude",
    ROUND(ST_Y(ST_Centroid("ZoneGeometry"))::numeric, 6) AS "Centroid_Latitude"
FROM dds."DimZone"
WHERE "HasGeometry" = TRUE
ORDER BY "Area_Km2" DESC
LIMIT 10;
