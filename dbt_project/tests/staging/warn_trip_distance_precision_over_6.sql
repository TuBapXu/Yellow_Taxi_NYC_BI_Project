{{ config(severity='warn') }}
-- B01 (Data Contract 2.1): trip_distance có hơn 6 chữ số thập phân phải được PHÁT HIỆN để review,
-- không âm thầm thay đổi. Test cảnh báo (warn) liệt kê các dòng bị làm tròn khi ép NUMERIC(18,6).
select trip_id, source_file_name, source_row_number, trip_distance_raw, trip_distance
from {{ ref('stg_yellow_trips') }}
where trip_distance_raw is not null
  and btrim(trip_distance_raw)::numeric <> trip_distance
