{{ config(enabled=var('b01_use_mock', false)) }}
-- B01: So khớp kết quả staging trên dữ liệu giả lập với bảng kỳ vọng (seed mock_expected_stg_yellow_trips).
-- Trả về dòng nào lệch: vào nhầm stg/quarantine, sai lý do, sai kiểu/giá trị, sai thời lượng.
with actual as (
    select source_file_name, source_row_number, true as in_stg, null::varchar as quarantine_reason,
           passenger_count, store_and_fwd_flag, trip_duration_seconds, vendor_id,
           rate_code_id, trip_distance, money_rounded_columns
    from {{ ref('stg_yellow_trips') }}
    union all
    select source_file_name, source_row_number, false, quarantine_reason,
           null, null, null, null, null, null, null
    from {{ ref('stg_yellow_trips_rejects') }}
),
expected as (
    select * from {{ ref('mock_expected_stg_yellow_trips') }}
)
select coalesce(e.source_file_name, a.source_file_name)   as source_file_name,
       coalesce(e.source_row_number, a.source_row_number) as source_row_number,
       e.expect_in_stg, a.in_stg,
       e.expect_quarantine_reason, a.quarantine_reason,
       e.expect_passenger_count, a.passenger_count,
       e.expect_store_and_fwd_flag, a.store_and_fwd_flag,
       e.expect_trip_duration_seconds, a.trip_duration_seconds,
       e.expect_vendor_id, a.vendor_id,
       e.expect_rate_code_id, a.rate_code_id,
       e.expect_trip_distance, a.trip_distance,
       e.expect_money_rounded_columns, a.money_rounded_columns
from expected e
full outer join actual a
  on a.source_file_name = e.source_file_name and a.source_row_number = e.source_row_number
where e.source_row_number is null
   or a.source_row_number is null
   or e.expect_in_stg is distinct from a.in_stg
   or e.expect_quarantine_reason is distinct from a.quarantine_reason
   or e.expect_passenger_count is distinct from a.passenger_count
   or e.expect_store_and_fwd_flag is distinct from a.store_and_fwd_flag
   or e.expect_trip_duration_seconds is distinct from a.trip_duration_seconds
   or e.expect_vendor_id is distinct from a.vendor_id
   or e.expect_rate_code_id is distinct from a.rate_code_id
   or e.expect_trip_distance is distinct from a.trip_distance
   or e.expect_money_rounded_columns is distinct from a.money_rounded_columns
