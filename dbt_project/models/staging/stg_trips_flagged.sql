{#
  B02 (nbinh) - staging.stg_trips_flagged: stg_yellow_trips + bộ cờ chất lượng (Data Contract v1.2 mục 2.2).
  - Preserve & Flag: giữ nguyên 100% dòng của stg_yellow_trips, chỉ thêm cờ (không lọc, không nhân dòng).
  - 3 cờ usable + has_dq_issue luôn TRUE/FALSE, không NULL.
  - data_quality_flags: mã lỗi usable (thời gian, cự ly, tiền); attribute_quality_flags: cảnh báo thuộc tính.
  - Mapping vendor/rate code lấy từ seed dùng chung với DimVendor/DimRateCode (S02); KHÔNG đọc bảng dimension
    để tránh vòng phụ thuộc.
  Đầu vào của T02 (FactTaxiTrip).
#}
with stg as (
    select * from {{ ref('stg_yellow_trips') }}
),

coded as (
    select
        stg.*,
        {{ dq_time_codes('stg') }}     as time_codes,
        {{ dq_distance_codes('stg') }} as distance_codes,
        {{ dq_amount_codes('stg') }}   as amount_codes,
        {{ dq_attribute_codes(
            'stg',
            'exists (select 1 from ' ~ ref('seed_vendor_mapping') ~ ' vm where vm.vendor_id = stg.vendor_id)',
            'exists (select 1 from ' ~ ref('seed_rate_code_mapping') ~ ' rm where rm.rate_code_id = stg.rate_code_id)'
        ) }}                           as attribute_codes
    from stg
)

select
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    trip_duration_seconds,
    store_and_fwd_flag_raw,
    money_rounded_columns,

    -- Bộ cờ chất lượng
    cardinality(time_codes) = 0                                             as is_time_usable,
    cardinality(distance_codes) = 0                                         as is_distance_usable,
    cardinality(amount_codes) = 0                                           as is_amount_usable,
    not (cardinality(time_codes) = 0
         and cardinality(distance_codes) = 0
         and cardinality(amount_codes) = 0)                                 as has_dq_issue,
    {{ dq_join_codes('time_codes || distance_codes || amount_codes') }}    as data_quality_flags,
    {{ dq_join_codes('attribute_codes') }}                                 as attribute_quality_flags,
    '{{ var("dq_rule_version") }}'::varchar(32)                             as quality_rule_version,

    -- Lineage
    source_file_name,
    source_file_sha256,
    source_row_number,
    load_batch_id,
    loaded_at_utc
from coded
