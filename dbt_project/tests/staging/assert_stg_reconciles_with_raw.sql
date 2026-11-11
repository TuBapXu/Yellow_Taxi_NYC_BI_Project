-- B01: Đối soát số dòng trên cùng phạm vi: raw = stg_yellow_trips + rejects (quarantine). Trả dòng nếu lệch.
with src as (select count(*) as n from {{ ref('stg_yellow_trips_source') }}),
     stg as (select count(*) as n from {{ ref('stg_yellow_trips') }}),
     rej as (select count(*) as n from {{ ref('stg_yellow_trips_rejects') }})
select src.n as raw_rows, stg.n as stg_rows, rej.n as reject_rows
from src, stg, rej
where src.n <> stg.n + rej.n
