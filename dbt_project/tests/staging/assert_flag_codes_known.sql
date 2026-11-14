-- B02: Mọi mã trong data_quality_flags / attribute_quality_flags phải thuộc danh mục Contract v1.2 mục 2.2,
-- và has_dq_issue phải nhất quán với ba cờ usable. Trả dòng vi phạm.
with codes as (
    select trip_id, 'data' as kind, unnest(string_to_array(data_quality_flags, ';')) as code
    from {{ ref('stg_trips_flagged') }}
    union all
    select trip_id, 'attribute', unnest(string_to_array(attribute_quality_flags, ';'))
    from {{ ref('stg_trips_flagged') }}
)
select trip_id, kind, code
from codes
where (kind = 'data' and code not in (
          'MISSING_TIME', 'DATE_OUT_OF_DOMAIN', 'TIME_REVERSED', 'DURATION_OVER_24H',
          'MISSING_DISTANCE', 'NEGATIVE_DISTANCE', 'ZERO_DISTANCE', 'DISTANCE_OUTLIER',
          'MISSING_TOTAL_AMOUNT', 'MISSING_FARE_AMOUNT', 'MISSING_TIP_AMOUNT',
          'NEGATIVE_TOTAL_AMOUNT', 'ZERO_TOTAL_AMOUNT', 'NEGATIVE_FARE', 'NEGATIVE_TIP',
          'AMOUNT_OVER_5000', 'INCONSISTENT_TOTAL'))
   or (kind = 'attribute' and code not in (
          'MISSING_PASSENGER_COUNT', 'INVALID_PASSENGER_COUNT', 'MISSING_VENDOR', 'UNMAPPED_VENDOR',
          'MISSING_RATE_CODE', 'UNMAPPED_RATE_CODE', 'MISSING_STORE_AND_FWD_FLAG',
          'INVALID_STORE_AND_FWD_FLAG', 'MONEY_ROUNDED'))
union all
select trip_id, 'has_dq_issue', 'INCONSISTENT_WITH_USABLE_FLAGS'
from {{ ref('stg_trips_flagged') }}
where has_dq_issue <> not (is_time_usable and is_distance_usable and is_amount_usable)
   or (has_dq_issue and data_quality_flags is null)
   or (not has_dq_issue and data_quality_flags is not null)
