-- Kiểm thử toàn vẹn tham chiếu 11 Khóa ngoại trên FactTaxiTrip (Zero Orphaned FKs)
select 'orphan_PickupDateKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimDate') }} d on f."PickupDateKey" = d."DateKey"
where d."DateKey" is null
having count(*) > 0

union all

select 'orphan_DropoffDateKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimDate') }} d on f."DropoffDateKey" = d."DateKey"
where d."DateKey" is null
having count(*) > 0

union all

select 'orphan_PickupTimeKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimTime') }} d on f."PickupTimeKey" = d."TimeKey"
where d."TimeKey" is null
having count(*) > 0

union all

select 'orphan_DropoffTimeKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimTime') }} d on f."DropoffTimeKey" = d."TimeKey"
where d."TimeKey" is null
having count(*) > 0

union all

select 'orphan_PickupZoneKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimZone') }} d on f."PickupZoneKey" = d."ZoneKey"
where d."ZoneKey" is null
having count(*) > 0

union all

select 'orphan_DropoffZoneKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimZone') }} d on f."DropoffZoneKey" = d."ZoneKey"
where d."ZoneKey" is null
having count(*) > 0

union all

select 'orphan_VendorKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimVendor') }} d on f."VendorKey" = d."VendorKey"
where d."VendorKey" is null
having count(*) > 0

union all

select 'orphan_PaymentTypeKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimPaymentType') }} d on f."PaymentTypeKey" = d."PaymentTypeKey"
where d."PaymentTypeKey" is null
having count(*) > 0

union all

select 'orphan_RateCodeKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimRateCode') }} d on f."RateCodeKey" = d."RateCodeKey"
where d."RateCodeKey" is null
having count(*) > 0

union all

select 'orphan_PassengerGroupKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimPassengerGroup') }} d on f."PassengerGroupKey" = d."PassengerGroupKey"
where d."PassengerGroupKey" is null
having count(*) > 0

union all

select 'orphan_DistanceBandKey' as failure, count(*) as bad_rows
from {{ ref('FactTaxiTrip') }} f
left join {{ ref('DimDistanceBand') }} d on f."DistanceBandKey" = d."DistanceBandKey"
where d."DistanceBandKey" is null
having count(*) > 0
