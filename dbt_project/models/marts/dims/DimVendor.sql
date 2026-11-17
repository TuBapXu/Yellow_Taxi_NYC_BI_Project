{{ config(
    materialized='table',
    alias='DimVendor',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"VendorKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_VendorKey_fkey', 'VendorKey', 'DimVendor', 'VendorKey') }}"
    ]
) }}

SELECT * FROM (
    VALUES
        (0::INT, 0::INT, 'Unknown / Unmapped Vendor'::VARCHAR(100), 'MISSING'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (1::INT, 1::INT, 'Creative Mobile Technologies, LLC'::VARCHAR(100), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (2::INT, 2::INT, 'VeriFone Inc.'::VARCHAR(100), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32))
) AS t("VendorKey", "VendorID", "VendorName", "MappingStatus", "MetadataVersion")
ORDER BY "VendorKey"