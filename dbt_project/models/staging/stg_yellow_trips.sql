{#
  B01 (nbinh) - staging.stg_yellow_trips: giao diện chuẩn hóa theo Data Contract v1.2 mục 2.1.
  Chỉ gồm các dòng chuyển kiểu thành công (dòng lỗi nằm ở stg_yellow_trips_rejects -> staging.quarantine).
  Cờ DQ (is_*_usable, flags) được bổ sung ở B02 (stg_trips_flagged).
#}
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
    trip_distance_raw,
    money_rounded_columns,
    source_file_name,
    source_file_sha256,
    source_row_number,
    load_batch_id,
    loaded_at_utc
from {{ ref('stg_yellow_trips_typed') }}
where not has_cast_error
