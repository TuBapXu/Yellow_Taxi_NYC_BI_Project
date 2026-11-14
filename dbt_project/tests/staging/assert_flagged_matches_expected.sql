{{ config(enabled=var('b01_use_mock', false)) }}
-- B02: So khớp bộ cờ trên dữ liệu giả lập với kỳ vọng tính tay theo Contract v1.2 mục 2.2 / mục 4.
--   mock_expected_flagged       : 18 dòng contract (file mock_yellow_trips.csv)
--   mock_expected_flagged_edge  : ca biên ép kiểu + ca biên DQ
-- Trả về dòng nào thiếu/thừa hoặc lệch một trong 6 cột cờ.
with expected as (
    select 'mock_yellow_trips.csv'::varchar(255) as source_file_name, source_row_number,
           is_time_usable, is_distance_usable, is_amount_usable, has_dq_issue,
           data_quality_flags, attribute_quality_flags
    from {{ ref('mock_expected_flagged') }}
    union all
    select source_file_name, source_row_number,
           is_time_usable, is_distance_usable, is_amount_usable, has_dq_issue,
           data_quality_flags, attribute_quality_flags
    from {{ ref('mock_expected_flagged_edge') }}
),
actual as (
    select source_file_name, source_row_number,
           is_time_usable, is_distance_usable, is_amount_usable, has_dq_issue,
           data_quality_flags, attribute_quality_flags
    from {{ ref('stg_trips_flagged') }}
)
select coalesce(e.source_file_name, a.source_file_name)   as source_file_name,
       coalesce(e.source_row_number, a.source_row_number) as source_row_number,
       e.is_time_usable as exp_time, a.is_time_usable as act_time,
       e.is_distance_usable as exp_dist, a.is_distance_usable as act_dist,
       e.is_amount_usable as exp_amount, a.is_amount_usable as act_amount,
       e.has_dq_issue as exp_issue, a.has_dq_issue as act_issue,
       e.data_quality_flags as exp_dq, a.data_quality_flags as act_dq,
       e.attribute_quality_flags as exp_attr, a.attribute_quality_flags as act_attr
from expected e
full outer join actual a
  on a.source_file_name = e.source_file_name and a.source_row_number = e.source_row_number
where e.source_row_number is null
   or a.source_row_number is null
   or e.is_time_usable          is distinct from a.is_time_usable
   or e.is_distance_usable      is distinct from a.is_distance_usable
   or e.is_amount_usable        is distinct from a.is_amount_usable
   or e.has_dq_issue            is distinct from a.has_dq_issue
   or e.data_quality_flags      is distinct from a.data_quality_flags
   or e.attribute_quality_flags is distinct from a.attribute_quality_flags
