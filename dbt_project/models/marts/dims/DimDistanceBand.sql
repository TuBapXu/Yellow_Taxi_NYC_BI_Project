{{ config(
    materialized='table',
    alias='DimDistanceBand',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"DistanceBandKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_DistanceBandKey_fkey', 'DistanceBandKey', 'DimDistanceBand', 'DistanceBandKey') }}"
    ]
) }}

SELECT * FROM (
    VALUES
        (0::INT, 'INVALID'::VARCHAR(30), 'Bất thường / Âm'::VARCHAR(50), NULL::NUMERIC(10,2), NULL::NUMERIC(10,2), 'miles'::VARCHAR(10), 6::INT, 'v1.0'::VARCHAR(32)),
        (1::INT, 'ZERO'::VARCHAR(30), '0 miles'::VARCHAR(50), 0.00::NUMERIC(10,2), 0.00::NUMERIC(10,2), 'miles'::VARCHAR(10), 1::INT, 'v1.0'::VARCHAR(32)),
        (2::INT, 'SHORT'::VARCHAR(30), '(0-2] miles'::VARCHAR(50), 0.00::NUMERIC(10,2), 2.00::NUMERIC(10,2), 'miles'::VARCHAR(10), 2::INT, 'v1.0'::VARCHAR(32)),
        (3::INT, 'MEDIUM'::VARCHAR(30), '(2-5] miles'::VARCHAR(50), 2.00::NUMERIC(10,2), 5.00::NUMERIC(10,2), 'miles'::VARCHAR(10), 3::INT, 'v1.0'::VARCHAR(32)),
        (4::INT, 'LONG'::VARCHAR(30), '(5-100] miles'::VARCHAR(50), 5.00::NUMERIC(10,2), 100.00::NUMERIC(10,2), 'miles'::VARCHAR(10), 4::INT, 'v1.0'::VARCHAR(32)),
        (5::INT, 'OUTLIER'::VARCHAR(30), '>100 miles'::VARCHAR(50), 100.00::NUMERIC(10,2), NULL::NUMERIC(10,2), 'miles'::VARCHAR(10), 5::INT, 'v1.0'::VARCHAR(32))
) AS t("DistanceBandKey", "DistanceBandCode", "DistanceBandName", "LowerBoundMiles", "UpperBoundMiles", "Unit", "SortOrder", "RuleVersion")
ORDER BY "DistanceBandKey"