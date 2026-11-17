-- Đối soát số dòng và tổng tiền giữa Fact và Staging theo từng file nguồn
with fact_summary as (
    select
        "SourceFileName",
        count(*) as fact_rows,
        round(coalesce(sum("TotalAmount"), 0), 2) as fact_amount
    from {{ ref('FactTaxiTrip') }}
    group by "SourceFileName"
),
stg_summary as (
    select
        source_file_name as "SourceFileName",
        count(*) as stg_rows,
        round(coalesce(sum(total_amount), 0), 2) as stg_amount
    from {{ ref('stg_trips_flagged') }}
    group by source_file_name
)

select
    coalesce(f."SourceFileName", s."SourceFileName") as source_file,
    f.fact_rows,
    s.stg_rows,
    f.fact_amount,
    s.stg_amount
from fact_summary f
full join stg_summary s on f."SourceFileName" = s."SourceFileName"
where f.fact_rows is distinct from s.stg_rows
   or f.fact_amount is distinct from s.stg_amount
