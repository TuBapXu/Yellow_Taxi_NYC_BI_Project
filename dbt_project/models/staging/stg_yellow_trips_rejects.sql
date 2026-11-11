{#
  B01 (nbinh) - Các dòng raw không chuyển kiểu được, đúng khuôn bảng staging.quarantine (T01).
  quarantine_reason: các nhóm lỗi không lặp, sắp alphabet, nối ';' (TYPE_CAST_ERROR, OVERFLOW_ERROR,
                     NON_FINITE_NUMBER, UNPARSABLE_TIMESTAMP).
  raw_payload: toàn bộ 19 giá trị gốc + "_cast_errors" chi tiết theo cột.
#}
with r as (
    select t.trip_id, t.source_file_name, t.source_file_sha256, t.source_row_number,
           t.load_batch_id, t.cast_errors
    from {{ ref('stg_yellow_trips_typed') }} t
    where t.has_cast_error
)

select
    r.trip_id,
    r.source_file_name,
    r.source_file_sha256,
    r.source_row_number,
    r.load_batch_id,
    (
        select string_agg(distinct split_part(e, ':', 1), ';' order by split_part(e, ':', 1))
        from unnest(r.cast_errors) as e
    )::varchar(100)                                                      as quarantine_reason,
    (to_jsonb(s) - array['source_file_name', 'source_file_sha256', 'source_row_number',
                         'load_batch_id', 'loaded_at_utc'])
        || jsonb_build_object('_cast_errors', to_jsonb(r.cast_errors))   as raw_payload
from r
join {{ ref('stg_yellow_trips_source') }} s
  on s.source_file_sha256 = r.source_file_sha256
 and s.source_row_number = r.source_row_number
