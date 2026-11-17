-- B02: Preserve & Flag - stg_trips_flagged phải giữ đúng số dòng và tổng tiền của stg_yellow_trips
-- (không lọc, không nhân dòng khi tra seed mapping). Trả dòng nếu lệch.
with s as (select count(*) as n, sum(total_amount) as amt from {{ ref('stg_yellow_trips') }}),
     f as (select count(*) as n, sum(total_amount) as amt from {{ ref('stg_trips_flagged') }})
select s.n as stg_rows, f.n as flagged_rows, s.amt as stg_total, f.amt as flagged_total
from s, f
where s.n <> f.n or s.amt is distinct from f.amt
