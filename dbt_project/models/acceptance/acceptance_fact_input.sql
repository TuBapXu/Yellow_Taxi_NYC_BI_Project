{# Acceptance-only projection of the T02 interface. Does NOT load FactTaxiTrip. #}
{{ config(enabled=var('integration_acceptance', false), materialized='table', schema='qa') }}
select f.trip_id::char(32) as "TripKey",
 coalesce(pd."DateKey", 0)::int as "PickupDateKey",
 coalesce(dd."DateKey", 0)::int as "DropoffDateKey",
 coalesce(pt."TimeKey", 0)::int as "PickupTimeKey",
 coalesce(dt."TimeKey", 0)::int as "DropoffTimeKey",
 coalesce(pz."ZoneKey", 0)::int as "PickupZoneKey",
 coalesce(dz."ZoneKey", 0)::int as "DropoffZoneKey",
 coalesce(v."VendorKey", 0)::int as "VendorKey",
 coalesce(p."PaymentTypeKey", 0)::int as "PaymentTypeKey",
 coalesce(r."RateCodeKey", 0)::int as "RateCodeKey",
 coalesce(pg."PassengerGroupKey", 0)::int as "PassengerGroupKey",
 coalesce(b."DistanceBandKey", 0)::int as "DistanceBandKey",
 f.pickup_datetime as "PickupDateTime",
 f.dropoff_datetime as "DropoffDateTime",
 f.store_and_fwd_flag as "StoreAndFwdFlag",
 1::int as "TripCount",
 f.passenger_count as "PassengerCount",
 f.trip_distance as "TripDistanceMiles",
 f.trip_duration_seconds as "TripDurationSeconds",
 f.fare_amount as "FareAmount",
 f.extra as "Extra",
 f.mta_tax as "MtaTax",
 f.tip_amount as "TipAmount",
 f.tolls_amount as "TollsAmount",
 f.improvement_surcharge as "ImprovementSurcharge",
 f.congestion_surcharge as "CongestionSurcharge",
 f.airport_fee as "AirportFee",
 f.total_amount as "TotalAmount",
 f.is_time_usable as "IsTimeUsable",
 f.is_distance_usable as "IsDistanceUsable",
 f.is_amount_usable as "IsAmountUsable",
 f.has_dq_issue as "HasDQIssue",
 f.data_quality_flags as "DataQualityFlags",
 f.attribute_quality_flags as "AttributeQualityFlags",
 f.quality_rule_version as "QualityRuleVersion",
 f.source_file_name as "SourceFileName",
 f.source_file_sha256 as "SourceFileSHA256",
 f.source_row_number as "SourceRowNumber",
 f.load_batch_id as "LoadBatchID",
 f.loaded_at_utc as "LoadedAtUTC"
from {{ ref('stg_trips_flagged') }} f
left join {{ ref('DimDate') }} pd on pd."FullDate" = f.pickup_datetime::date
left join {{ ref('DimDate') }} dd on dd."FullDate" = f.dropoff_datetime::date
left join {{ ref('DimTime') }} pt on pt."TimeKey" = extract(hour from f.pickup_datetime)::int * 60 + extract(minute from f.pickup_datetime)::int + 1
left join {{ ref('DimTime') }} dt on dt."TimeKey" = extract(hour from f.dropoff_datetime)::int * 60 + extract(minute from f.dropoff_datetime)::int + 1
left join {{ ref('DimZone') }} pz on pz."LocationID" = f.pu_location_id
left join {{ ref('DimZone') }} dz on dz."LocationID" = f.do_location_id
left join {{ ref('DimVendor') }} v on v."VendorID" = f.vendor_id
left join {{ ref('DimPaymentType') }} p on p."PaymentTypeCode" = f.payment_type
left join {{ ref('DimRateCode') }} r on r."RateCodeID" = f.rate_code_id
left join {{ ref('DimPassengerGroup') }} pg on f.passenger_count between pg."MinPassengerCount" and pg."MaxPassengerCount"
left join {{ ref('DimDistanceBand') }} b on b."DistanceBandKey" = case when f.trip_distance is null or f.trip_distance < 0 then 0 when f.trip_distance = 0 then 1 when f.trip_distance <= 2 then 2 when f.trip_distance <= 5 then 3 when f.trip_distance <= 100 then 4 else 5 end
