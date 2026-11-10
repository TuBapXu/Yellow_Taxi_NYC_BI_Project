{#
  B01 (nbinh) - Đầu vào của tầng staging.
  - Mặc định: bảng raw thật staging.raw_yellow_trips (S01 nạp).
  - Chạy với --vars '{b01_use_mock: true}': dùng 2 seed giả lập (18 dòng contract + ca biên ép kiểu),
    sinh lineage giống hệt raw để kiểm thử mà không đụng dữ liệu thật.
#}
{{ config(materialized='ephemeral') }}

{% set raw_cols = [
    'vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
    'trip_distance', 'rate_code_id', 'store_and_fwd_flag', 'pu_location_id', 'do_location_id',
    'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
    'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee'
] %}

{% if var('b01_use_mock', false) %}

    {% for seed_name in ['mock_yellow_trips', 'mock_cast_edge_cases'] %}
    select
        '{{ seed_name }}.csv'::varchar(255)                                        as source_file_name,
        encode(sha256(convert_to('{{ seed_name }}.csv', 'UTF8')), 'hex')::char(64) as source_file_sha256,
        source_row_number::bigint                                                  as source_row_number,
        'MOCK_SEED'::varchar(64)                                                   as load_batch_id,
        (now() at time zone 'UTC')::timestamp                                      as loaded_at_utc,
        {% for c in raw_cols %}
        {{ c }}::varchar(50) as {{ c }}{{ "," if not loop.last }}
        {% endfor %}
    from {{ ref(seed_name) }}
    {{ "union all" if not loop.last }}
    {% endfor %}

{% else %}

    select
        source_file_name, source_file_sha256, source_row_number, load_batch_id, loaded_at_utc,
        {{ raw_cols | join(', ') }}
    from {{ source('staging', 'raw_yellow_trips') }}

{% endif %}
