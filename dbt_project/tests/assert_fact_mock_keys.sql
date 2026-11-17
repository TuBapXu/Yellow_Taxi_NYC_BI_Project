-- Kiểm thử 18 bộ khóa Dimension kỳ vọng trên dữ liệu giả lập (Contract v1.2)
{{ config(enabled=var('b01_use_mock', false)) }}

select coalesce(e.source_row_number, a."SourceRowNumber") as source_row_number
from {{ ref('mock_expected_flagged') }} e
full join (
    select * from {{ ref('FactTaxiTrip') }}
    where "SourceFileName" = 'mock_yellow_trips.csv'
) a
    on e.source_row_number = a."SourceRowNumber"
where e.source_row_number is null
   or a."SourceRowNumber" is null
   or a."PickupDateKey" is distinct from e.pickup_date_key
   or a."DropoffDateKey" is distinct from e.dropoff_date_key
   or a."PickupTimeKey" is distinct from e.pickup_time_key
   or a."DropoffTimeKey" is distinct from e.dropoff_time_key
   or a."PickupZoneKey" is distinct from e.pickup_zone_key
   or a."DropoffZoneKey" is distinct from e.dropoff_zone_key
   or a."VendorKey" is distinct from e.vendor_key
   or a."PaymentTypeKey" is distinct from e.payment_type_key
   or a."RateCodeKey" is distinct from e.rate_code_key
   or a."PassengerGroupKey" is distinct from e.passenger_group_key
   or a."DistanceBandKey" is distinct from e.distance_band_key
