{{ config(
    materialized='table',
    alias='DimVendor',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"VendorKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_VendorKey_fkey', 'VendorKey', 'DimVendor', 'VendorKey') }}"
    ]
) }}

select 0::int as "VendorKey", 0::int as "VendorID",
       'Unknown / Unmapped Vendor'::varchar(100) as "VendorName",
       'MISSING'::varchar(20) as "MappingStatus", '{{ var("dq_rule_version") }}'::varchar(32) as "MetadataVersion"
union all
select m.vendor_key::int, m.vendor_id::int, m.vendor_name::varchar(100),
       m.mapping_status::varchar(20), m.mapping_version::varchar(32)
from {{ ref('seed_vendor_mapping') }} m
where {{ mapping_is_active('m') }}
