{{ config(enabled=var('integration_acceptance', false)) }}
select 'orphan_PickupDateKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimDate') }} d on a."PickupDateKey"=d."DateKey" where d."DateKey" is null having count(*) > 0
union all
select 'orphan_DropoffDateKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimDate') }} d on a."DropoffDateKey"=d."DateKey" where d."DateKey" is null having count(*) > 0
union all
select 'orphan_PickupTimeKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimTime') }} d on a."PickupTimeKey"=d."TimeKey" where d."TimeKey" is null having count(*) > 0
union all
select 'orphan_DropoffTimeKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimTime') }} d on a."DropoffTimeKey"=d."TimeKey" where d."TimeKey" is null having count(*) > 0
union all
select 'orphan_PickupZoneKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimZone') }} d on a."PickupZoneKey"=d."ZoneKey" where d."ZoneKey" is null having count(*) > 0
union all
select 'orphan_DropoffZoneKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimZone') }} d on a."DropoffZoneKey"=d."ZoneKey" where d."ZoneKey" is null having count(*) > 0
union all
select 'orphan_VendorKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimVendor') }} d on a."VendorKey"=d."VendorKey" where d."VendorKey" is null having count(*) > 0
union all
select 'orphan_PaymentTypeKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimPaymentType') }} d on a."PaymentTypeKey"=d."PaymentTypeKey" where d."PaymentTypeKey" is null having count(*) > 0
union all
select 'orphan_RateCodeKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimRateCode') }} d on a."RateCodeKey"=d."RateCodeKey" where d."RateCodeKey" is null having count(*) > 0
union all
select 'orphan_PassengerGroupKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimPassengerGroup') }} d on a."PassengerGroupKey"=d."PassengerGroupKey" where d."PassengerGroupKey" is null having count(*) > 0
union all
select 'orphan_DistanceBandKey' as failure, count(*) as bad_rows from {{ ref('acceptance_fact_input') }} a left join {{ ref('DimDistanceBand') }} d on a."DistanceBandKey"=d."DistanceBandKey" where d."DistanceBandKey" is null having count(*) > 0
union all
select 'trip_id', count(*) from {{ ref('acceptance_fact_input') }}
where "TripKey" is distinct from md5(lower("SourceFileSHA256") || '_' || "SourceRowNumber"::text)
having count(*) > 0
union all
select 'duplicate_trip', count(*) - count(distinct "TripKey") from {{ ref('acceptance_fact_input') }}
having count(*) <> count(distinct "TripKey")
union all
select 'vendor_flag_key', count(*) from {{ ref('acceptance_fact_input') }}
where (coalesce("AttributeQualityFlags", '') ~ '(^|;)(MISSING_VENDOR|UNMAPPED_VENDOR)(;|$)')
is distinct from ("VendorKey"=0) having count(*) > 0
union all
select 'rate_flag_key', count(*) from {{ ref('acceptance_fact_input') }}
where (coalesce("AttributeQualityFlags", '') ~ '(^|;)(MISSING_RATE_CODE|UNMAPPED_RATE_CODE)(;|$)')
is distinct from ("RateCodeKey"=0) having count(*) > 0
union all
select 'count_money_reconciliation', 1
where (select row(count(*),sum("TotalAmount")) from {{ ref('acceptance_fact_input') }})
is distinct from (select row(count(*),sum(total_amount)) from {{ ref('stg_trips_flagged') }})
union all
select 'dq_passthrough', count(*) from {{ ref('acceptance_fact_input') }} a
join {{ ref('stg_trips_flagged') }} f on a."TripKey"=f.trip_id
where row(a."IsTimeUsable",a."IsDistanceUsable",a."IsAmountUsable",a."HasDQIssue",
 a."DataQualityFlags",a."AttributeQualityFlags",a."QualityRuleVersion")
is distinct from row(f.is_time_usable,f.is_distance_usable,f.is_amount_usable,f.has_dq_issue,
 f.data_quality_flags,f.attribute_quality_flags,f.quality_rule_version)
having count(*) > 0
