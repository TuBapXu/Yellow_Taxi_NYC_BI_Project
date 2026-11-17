-- Đối soát số dòng và tổng tiền giữa Fact và Staging theo từng file nguồn hiện có trong Staging.
-- Ghi chú: Trong kiến trúc Transient Staging Buffer, Staging chỉ lưu file đang xử lý.
-- Phép đối soát này chỉ so sánh các file đang có mặt trong Staging.
with stg_summary as (
    select
        source_file_name as "SourceFileName",
        count(*) as stg_rows,
        round(coalesce(sum(total_amount), 0), 2) as stg_amount
    from {{ ref('stg_trips_flagged') }}
    group by source_file_name
),
fact_summary as (
    select
        "SourceFileName",
        count(*) as fact_rows,
        round(coalesce(sum("TotalAmount"), 0), 2) as fact_amount
    from {{ ref('FactTaxiTrip') }}
    where "SourceFileName" in (select distinct "SourceFileName" from stg_summary)
    group by "SourceFileName"
)

select
    s."SourceFileName" as source_file,
    f.fact_rows,
    s.stg_rows,
    f.fact_amount,
    s.stg_amount
from stg_summary s
left join fact_summary f on f."SourceFileName" = s."SourceFileName"
where f.fact_rows is distinct from s.stg_rows
   or f.fact_amount is distinct from s.stg_amount
