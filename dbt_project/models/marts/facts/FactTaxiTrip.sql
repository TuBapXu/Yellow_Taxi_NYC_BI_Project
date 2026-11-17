{{ config(
    materialized='incremental',
    alias='FactTaxiTrip',
    quoting={'identifier': true},
    unique_key='"TripKey"',
    incremental_strategy='delete+insert'
) }}

{#
  TASK T02 (TTU) - MÔ HÌNH FACT TRUNG TÂM dds."FactTaxiTrip" (SINGLE DDS)
  - Grain: 1 dòng / 1 giao dịch chuyến xe riêng lẻ kết thúc thành công
  - 11 Role-playing Foreign Keys với quy ước khóa mặc định Key = 0 (Zero Orphaned FKs)
  - 40 Cột chuẩn hóa 100% theo Data Contract v1.2 & DDL PostgreSQL 16
  - Incremental: delete+insert theo TripKey đảm bảo Idempotent khi rerun batch
#}

with flagged_trips as (
    select *
    from {{ ref('stg_trips_flagged') }} f
    {% if is_incremental() and not var('b01_use_mock', false) and not var('fact_full_reload', false) %}
        {% if var('source_file_name', none) is not none %}
            where f.source_file_name = '{{ var("source_file_name") }}'
        {% elif var('load_batch_id', none) is not none %}
            where f.load_batch_id = '{{ var("load_batch_id") }}'
        {% else %}
            where f.loaded_at_utc >= (select coalesce(max("LoadedAtUTC"), '1970-01-01'::timestamp) from {{ this }})
        {% endif %}
    {% endif %}
),

dim_date_pickup as (
    select "DateKey", "FullDate" from {{ ref('DimDate') }}
),

dim_date_dropoff as (
    select "DateKey", "FullDate" from {{ ref('DimDate') }}
),

dim_time_pickup as (
    select "TimeKey" from {{ ref('DimTime') }}
),

dim_time_dropoff as (
    select "TimeKey" from {{ ref('DimTime') }}
),

dim_zone_pickup as (
    select "ZoneKey", "LocationID" from {{ ref('DimZone') }}
),

dim_zone_dropoff as (
    select "ZoneKey", "LocationID" from {{ ref('DimZone') }}
),

dim_vendor as (
    select "VendorKey", "VendorID" from {{ ref('DimVendor') }}
),

dim_payment as (
    select "PaymentTypeKey", "PaymentTypeCode" from {{ ref('DimPaymentType') }}
),

dim_rate as (
    select "RateCodeKey", "RateCodeID" from {{ ref('DimRateCode') }}
),

dim_passenger_group as (
    select "PassengerGroupKey", "MinPassengerCount", "MaxPassengerCount" from {{ ref('DimPassengerGroup') }}
),

dim_distance_band as (
    select "DistanceBandKey" from {{ ref('DimDistanceBand') }}
)

select
    -- 1. Khóa chính bất biến (Primary Key)
    f.trip_id::char(32)                                                     as "TripKey",

    -- 2. Hệ thống 11 Khóa ngoại Role-playing (Bắt buộc NOT NULL, Mặc định = 0)
    coalesce(pd."DateKey", 0)::int                                         as "PickupDateKey",
    coalesce(dd."DateKey", 0)::int                                         as "DropoffDateKey",
    coalesce(pt."TimeKey", 0)::int                                         as "PickupTimeKey",
    coalesce(dt."TimeKey", 0)::int                                         as "DropoffTimeKey",
    coalesce(pz."ZoneKey", 0)::int                                         as "PickupZoneKey",
    coalesce(dz."ZoneKey", 0)::int                                         as "DropoffZoneKey",
    coalesce(v."VendorKey", 0)::int                                        as "VendorKey",
    coalesce(p."PaymentTypeKey", 0)::int                                   as "PaymentTypeKey",
    coalesce(r."RateCodeKey", 0)::int                                      as "RateCodeKey",
    coalesce(pg."PassengerGroupKey", 0)::int                               as "PassengerGroupKey",
    coalesce(b."DistanceBandKey", 0)::int                                  as "DistanceBandKey",

    -- 3. Thuộc tính nguồn giữ lại phục vụ kiểm toán
    f.pickup_datetime                                                      as "PickupDateTime",
    f.dropoff_datetime                                                     as "DropoffDateTime",
    f.store_and_fwd_flag                                                   as "StoreAndFwdFlag",

    -- 4. Hệ thống 13 Thước đo định lượng (Base Measures)
    1::int                                                                 as "TripCount",
    f.passenger_count                                                      as "PassengerCount",
    f.trip_distance                                                        as "TripDistanceMiles",
    f.trip_duration_seconds                                                as "TripDurationSeconds",
    f.fare_amount                                                          as "FareAmount",
    f.extra                                                                as "Extra",
    f.mta_tax                                                              as "MtaTax",
    f.tip_amount                                                           as "TipAmount",
    f.tolls_amount                                                         as "TollsAmount",
    f.improvement_surcharge                                                as "ImprovementSurcharge",
    f.congestion_surcharge                                                 as "CongestionSurcharge",
    f.airport_fee                                                          as "AirportFee",
    f.total_amount                                                         as "TotalAmount",

    -- 5. Khung cờ chất lượng dữ liệu (Data Quality Framework - Preserve & Flag)
    f.is_time_usable                                                       as "IsTimeUsable",
    f.is_distance_usable                                                   as "IsDistanceUsable",
    f.is_amount_usable                                                     as "IsAmountUsable",
    f.has_dq_issue                                                         as "HasDQIssue",
    f.data_quality_flags                                                   as "DataQualityFlags",
    f.attribute_quality_flags                                              as "AttributeQualityFlags",
    f.quality_rule_version                                                 as "QualityRuleVersion",

    -- 6. Thuộc tính truy vết nguồn gốc (Data Lineage & Chống trùng lặp)
    f.source_file_name                                                     as "SourceFileName",
    f.source_file_sha256                                                   as "SourceFileSHA256",
    f.source_row_number                                                    as "SourceRowNumber",
    f.load_batch_id                                                        as "LoadBatchID",
    f.loaded_at_utc                                                        as "LoadedAtUTC"

from flagged_trips f
left join dim_date_pickup pd
    on pd."FullDate" = f.pickup_datetime::date
left join dim_date_dropoff dd
    on dd."FullDate" = f.dropoff_datetime::date
left join dim_time_pickup pt
    on pt."TimeKey" = extract(hour from f.pickup_datetime)::int * 60 + extract(minute from f.pickup_datetime)::int + 1
left join dim_time_dropoff dt
    on dt."TimeKey" = extract(hour from f.dropoff_datetime)::int * 60 + extract(minute from f.dropoff_datetime)::int + 1
left join dim_zone_pickup pz
    on pz."LocationID" = f.pu_location_id
left join dim_zone_dropoff dz
    on dz."LocationID" = f.do_location_id
left join dim_vendor v
    on v."VendorID" = f.vendor_id
left join dim_payment p
    on p."PaymentTypeCode" = f.payment_type
left join dim_rate r
    on r."RateCodeID" = f.rate_code_id
left join dim_passenger_group pg
    on f.passenger_count between pg."MinPassengerCount" and pg."MaxPassengerCount"
left join dim_distance_band b
    on b."DistanceBandKey" = case
        when f.trip_distance is null or f.trip_distance < 0 then 0
        when f.trip_distance = 0 then 1
        when f.trip_distance <= 2 then 2
        when f.trip_distance <= 5 then 3
        when f.trip_distance <= 100 then 4
        else 5
    end
