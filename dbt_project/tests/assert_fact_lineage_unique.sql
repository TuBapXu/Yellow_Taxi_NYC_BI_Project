-- Kiểm thử tính duy nhất của cặp Lineage (SourceFileSHA256, SourceRowNumber) trên Fact
select
    "SourceFileSHA256",
    "SourceRowNumber",
    count(*) as duplicate_count
from {{ ref('FactTaxiTrip') }}
group by "SourceFileSHA256", "SourceRowNumber"
having count(*) > 1
