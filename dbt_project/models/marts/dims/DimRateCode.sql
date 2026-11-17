{{ config(
    materialized='table',
    alias='DimRateCode',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"RateCodeKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_RateCodeKey_fkey', 'RateCodeKey', 'DimRateCode', 'RateCodeKey') }}"
    ]
) }}

SELECT * FROM (
    VALUES
        (0::INT, 0::INT, 'Unknown / Unmapped Rate'::VARCHAR(100), 'OTHER'::VARCHAR(50), 'MISSING'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (1::INT, 1::INT, 'Standard rate'::VARCHAR(100), 'STANDARD'::VARCHAR(50), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (2::INT, 2::INT, 'JFK'::VARCHAR(100), 'AIRPORT'::VARCHAR(50), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (3::INT, 3::INT, 'Newark'::VARCHAR(100), 'AIRPORT'::VARCHAR(50), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (4::INT, 4::INT, 'Nassau or Westchester'::VARCHAR(100), 'OTHER'::VARCHAR(50), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (5::INT, 5::INT, 'Negotiated fare'::VARCHAR(100), 'NEGOTIATED'::VARCHAR(50), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (6::INT, 6::INT, 'Group ride'::VARCHAR(100), 'OTHER'::VARCHAR(50), 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (99::INT, 99::INT, 'Other / Unknown Rate 99'::VARCHAR(100), 'OTHER'::VARCHAR(50), 'UNMAPPED'::VARCHAR(20), 'v1.0'::VARCHAR(32))
) AS t("RateCodeKey", "RateCodeID", "RateCodeName", "RateGroup", "MappingStatus", "MetadataVersion")
ORDER BY "RateCodeKey"