-- B01: Mỗi (source_file_sha256, source_row_number) chỉ xuất hiện 1 lần ở staging (không nhân dòng).
select source_file_sha256, source_row_number, count(*) as n
from {{ ref('stg_yellow_trips') }}
group by 1, 2
having count(*) > 1
