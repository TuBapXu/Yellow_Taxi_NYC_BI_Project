-- Both directions: dimensions contain exactly the active seed rows plus key 0.
with expected as (
 select 'vendor' as kind, vendor_key as key, vendor_id as id, vendor_name as name,
 mapping_status as status, mapping_version as version
 from {{ ref('seed_vendor_mapping') }} m where {{ mapping_is_active('m') }}
 union all
 select 'rate', rate_code_key, rate_code_id, rate_code_name, mapping_status, mapping_version
 from {{ ref('seed_rate_code_mapping') }} m where {{ mapping_is_active('m') }}
), actual as (
 select 'vendor' as kind, "VendorKey" as key, "VendorID" as id, "VendorName" as name,
 "MappingStatus" as status, "MetadataVersion" as version
 from {{ ref('DimVendor') }} where "VendorKey" <> 0
 union all
 select 'rate', "RateCodeKey", "RateCodeID", "RateCodeName", "MappingStatus", "MetadataVersion"
 from {{ ref('DimRateCode') }} where "RateCodeKey" <> 0
)
(select * from expected except select * from actual)
union all
(select * from actual except select * from expected)
