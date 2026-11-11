{#
  B01 (nbinh) - Ép kiểu an toàn TOÀN BỘ dòng raw + ghi nhận lỗi chuyển kiểu theo từng cột.
  Hạ nguồn:
    stg_yellow_trips          = các dòng không có lỗi chuyển kiểu (đầu vào cho B02 stg_trips_flagged)
    stg_yellow_trips_rejects  = các dòng lỗi -> macro load_quarantine() ghi vào staging.quarantine
  Đối soát: COUNT(raw) = COUNT(stg_yellow_trips) + COUNT(stg_yellow_trips_rejects)
#}
{% set int_cols = [
    ('vendor_id', 'vendor_id'), ('passenger_count', 'passenger_count'),
    ('rate_code_id', 'rate_code_id'), ('pu_location_id', 'pu_location_id'),
    ('do_location_id', 'do_location_id'), ('payment_type', 'payment_type')
] %}
{% set money_cols = [
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
    'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'total_amount'
] %}

with src as (
    select * from {{ ref('stg_yellow_trips_source') }}
),

typed as (
    select
        -- Khóa giao dịch: MD5(sha256 || '_' || số dòng) - hex chữ thường, CHAR(32)
        md5(src.source_file_sha256::text || '_' || src.source_row_number::text)::char(32) as trip_id,

        {{ safe_int('src.vendor_id') }}                        as vendor_id,
        {{ safe_timestamp('src.tpep_pickup_datetime') }}       as pickup_datetime,
        {{ safe_timestamp('src.tpep_dropoff_datetime') }}      as dropoff_datetime,
        {{ safe_int('src.passenger_count') }}                  as passenger_count,
        {{ safe_numeric('src.trip_distance', 18, 6) }}         as trip_distance,
        {{ safe_int('src.rate_code_id') }}                     as rate_code_id,
        -- Chuẩn hóa 'Y'/'N' (không phân biệt hoa thường, bỏ khoảng trắng); giá trị khác -> NULL
        case upper({{ clean_text('src.store_and_fwd_flag') }})
            when 'Y' then 'Y' when 'N' then 'N'
        end::varchar(1)                                        as store_and_fwd_flag,
        {{ safe_int('src.pu_location_id') }}                   as pu_location_id,
        {{ safe_int('src.do_location_id') }}                   as do_location_id,
        {{ safe_int('src.payment_type') }}                     as payment_type,
        {% for c in money_cols %}
        {{ safe_numeric('src.' ~ c, 18, 2) }}                  as {{ c }},
        {% endfor %}

        -- Giá trị gốc giữ lại để B02 gắn INVALID_STORE_AND_FWD_FLAG / MONEY_ROUNDED / review độ chính xác cự ly
        {{ clean_text('src.store_and_fwd_flag') }}               as store_and_fwd_flag_raw,
        {{ clean_text('src.trip_distance') }}                    as trip_distance_raw,

        -- Giá trị tiền gốc (đã làm sạch) để phát hiện làm tròn ở bước sau, không xuất ra stg_yellow_trips
        {% for c in money_cols %}
        {{ clean_text('src.' ~ c) }} as raw_{{ c }},
        {% endfor %}

        -- Lineage
        src.source_file_name::varchar(255)                     as source_file_name,
        src.source_file_sha256::char(64)                       as source_file_sha256,
        src.source_row_number::bigint                          as source_row_number,
        src.load_batch_id::varchar(64)                         as load_batch_id,
        src.loaded_at_utc::timestamp                           as loaded_at_utc,

        -- Mã lỗi chuyển kiểu theo cột: mảng 'MA_LOI:ten_cot', bỏ phần tử NULL
        array_remove(array[
            {{ int_error('src.vendor_id') }} || ':vendor_id',
            {{ timestamp_error('src.tpep_pickup_datetime') }} || ':tpep_pickup_datetime',
            {{ timestamp_error('src.tpep_dropoff_datetime') }} || ':tpep_dropoff_datetime',
            {{ int_error('src.passenger_count') }} || ':passenger_count',
            {{ numeric_error('src.trip_distance', 18, 6) }} || ':trip_distance',
            {{ int_error('src.rate_code_id') }} || ':rate_code_id',
            {{ int_error('src.pu_location_id') }} || ':pu_location_id',
            {{ int_error('src.do_location_id') }} || ':do_location_id',
            {{ int_error('src.payment_type') }} || ':payment_type',
            {% for c in money_cols %}
            {{ numeric_error('src.' ~ c, 18, 2) }} || ':{{ c }}'{{ "," if not loop.last }}
            {% endfor %}
        ], null)                                               as cast_errors
    from src
)

select
    typed.*,
    -- Thời lượng chuyến (giây, giữ phần lẻ). Phép trừ timestamp: không có phép chia -> không thể lỗi chia cho 0.
    -- NULL khi thiếu một trong hai mốc; có thể âm (TIME_REVERSED do B02 gắn cờ, không xóa dòng).
    extract(epoch from (typed.dropoff_datetime - typed.pickup_datetime))::numeric(20,6) as trip_duration_seconds,
    cardinality(typed.cast_errors) > 0                                                  as has_cast_error,
    -- Tiền bị làm tròn khi chuẩn hóa về cent (Contract 2.2: giữ raw, B02 gắn MONEY_ROUNDED):
    -- các cột tiền có giá trị gốc khác giá trị sau làm tròn, sắp alphabet, nối ';'; không có thì NULL.
    -- Nhánh nhanh: chuỗi gốc có tối đa 2 chữ số thập phân thì chắc chắn không bị làm tròn.
    nullif(concat_ws(';'
        {% for c in money_cols | sort %}
        , case when typed.{{ c }} is null then null
               when typed.raw_{{ c }} ~ '^[+-]?[0-9]*(\.[0-9]{0,2})?$' then null
               when typed.raw_{{ c }}::numeric <> typed.{{ c }} then '{{ c }}'
          end
        {% endfor %}
    ), '')                                                                              as money_rounded_columns
from typed
