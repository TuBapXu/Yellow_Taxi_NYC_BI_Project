{{ config(
    materialized='table',
    alias='DimVendor',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"VendorKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_VendorKey_fkey', 'VendorKey', 'DimVendor', 'VendorKey') }}"
    ]
) }}

WITH unknown_member AS (
    SELECT
        0::INT AS "VendorKey",
        0::INT AS "VendorID",
        'Unknown / Unmapped Vendor'::VARCHAR(100) AS "VendorName",
        'MISSING'::VARCHAR(20) AS "MappingStatus",
        'v1.2'::VARCHAR(32) AS "MetadataVersion"
),
seeded_vendors AS (
    SELECT
        vendor_key::INT AS "VendorKey",
        vendor_id::INT AS "VendorID",
        vendor_name::VARCHAR(100) AS "VendorName",
        mapping_status::VARCHAR(20) AS "MappingStatus",
        mapping_version::VARCHAR(32) AS "MetadataVersion"
    FROM {{ ref('seed_vendor_mapping') }}
    WHERE mapping_status = 'VERIFIED'
)
SELECT * FROM unknown_member
UNION ALL
SELECT * FROM seeded_vendors
ORDER BY "VendorKey"