{{ config(
    materialized='table',
    alias='DimPassengerGroup',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"PassengerGroupKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_PassengerGroupKey_fkey', 'PassengerGroupKey', 'DimPassengerGroup', 'PassengerGroupKey') }}"
    ]
) }}

SELECT * FROM (
    VALUES
        (0::INT, 'INVALID_OR_MISSING'::VARCHAR(30), 'Bất thường / Khuyết'::VARCHAR(50), NULL::INT, NULL::INT, 4::INT, 'v1.0'::VARCHAR(32)),
        (1::INT, 'SOLO'::VARCHAR(30), '1 khách'::VARCHAR(50), 1::INT, 1::INT, 1::INT, 'v1.0'::VARCHAR(32)),
        (2::INT, 'GROUP_2_4'::VARCHAR(30), '2-4 khách'::VARCHAR(50), 2::INT, 4::INT, 2::INT, 'v1.0'::VARCHAR(32)),
        (3::INT, 'GROUP_5_PLUS'::VARCHAR(30), '5+ khách'::VARCHAR(50), 5::INT, 9::INT, 3::INT, 'v1.0'::VARCHAR(32))
) AS t("PassengerGroupKey", "PassengerGroupCode", "PassengerGroupName", "MinPassengerCount", "MaxPassengerCount", "SortOrder", "RuleVersion")
ORDER BY "PassengerGroupKey"