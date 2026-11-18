{{ config(
    materialized='table',
    alias='DimRateCode',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"RateCodeKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_RateCodeKey_fkey', 'RateCodeKey', 'DimRateCode', 'RateCodeKey') }}"
    ]
) }}

WITH unknown_member AS (
    SELECT
        0::INT AS "RateCodeKey",
        0::INT AS "RateCodeID",
        'Unknown / Unmapped Rate'::VARCHAR(100) AS "RateCodeName",
        'OTHER'::VARCHAR(50) AS "RateGroup",
        'MISSING'::VARCHAR(20) AS "MappingStatus",
        'v1.2'::VARCHAR(32) AS "MetadataVersion"
),
seeded_rates AS (
    SELECT
        rate_code_key::INT AS "RateCodeKey",
        rate_code_id::INT AS "RateCodeID",
        rate_code_name::VARCHAR(100) AS "RateCodeName",
        UPPER(rate_group)::VARCHAR(50) AS "RateGroup",
        mapping_status::VARCHAR(20) AS "MappingStatus",
        mapping_version::VARCHAR(32) AS "MetadataVersion"
    FROM {{ ref('seed_rate_code_mapping') }}
    WHERE mapping_status = 'VERIFIED'
       OR ({{ var('b01_use_mock', false) }} = TRUE AND mapping_status = 'FIXTURE')
)
SELECT * FROM unknown_member
UNION ALL
SELECT * FROM seeded_rates
ORDER BY "RateCodeKey"