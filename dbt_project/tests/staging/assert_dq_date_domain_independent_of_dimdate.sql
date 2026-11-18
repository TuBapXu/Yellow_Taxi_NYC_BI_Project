-- B02 (nbinh) - RFC-002 v2, điều kiện 1: mở rộng DimDate (dim_date_*) KHÔNG được làm đổi luật DQ (dq_date_domain_*).
-- Chạy được cả với dữ liệu thật lẫn mock (không đọc staging). Trả về dòng vi phạm, 0 dòng = PASS.
with probe as (
    -- Chuyến năm 2028 nằm trong file 2023: DimDate có ngày này nhưng luật DQ vẫn phải gắn DATE_OUT_OF_DOMAIN
    select 'trip_2028_in_2023_file'::text as check_name,
           timestamp '2028-06-15 10:00:00' as pickup_datetime,
           timestamp '2028-06-15 10:20:00' as dropoff_datetime,
           1200::int as trip_duration_seconds,
           true as expect_out_of_domain
    union all
    -- Chuyến hợp lệ giữa miền: không được gắn DATE_OUT_OF_DOMAIN
    select 'trip_2022_in_domain', timestamp '2022-03-15 10:00:00', timestamp '2022-03-15 10:20:00', 1200, false
)
select p.check_name, 'DQ_FLAG_MISMATCH' as problem
from probe p
where ('DATE_OUT_OF_DOMAIN' = any({{ dq_time_codes('p') }})) is distinct from p.expect_out_of_domain

union all
-- DimDate phải phủ đến dim_date_end (để các tháng 2024+ có DateKey thật, không rơi về Key 0)
select 'dimdate_covers_dim_date_end', 'DIMDATE_MISSING_END_DATE'
where not exists (
    select 1 from {{ ref('DimDate') }} where "FullDate" = date '{{ var("dim_date_end") }}'
)
