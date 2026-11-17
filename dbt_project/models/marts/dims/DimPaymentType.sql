{{ config(
    materialized='table',
    alias='DimPaymentType',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"PaymentTypeKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_PaymentTypeKey_fkey', 'PaymentTypeKey', 'DimPaymentType', 'PaymentTypeKey') }}"
    ]
) }}

SELECT * FROM (
    VALUES
        (0::INT, 0::INT, 'Unknown / Missing Payment'::VARCHAR(50), FALSE::BOOLEAN, 'MISSING'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (1::INT, 1::INT, 'Credit card'::VARCHAR(50), TRUE::BOOLEAN, 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (2::INT, 2::INT, 'Cash'::VARCHAR(50), FALSE::BOOLEAN, 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (3::INT, 3::INT, 'No charge'::VARCHAR(50), FALSE::BOOLEAN, 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (4::INT, 4::INT, 'Dispute'::VARCHAR(50), FALSE::BOOLEAN, 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (5::INT, 5::INT, 'Unknown'::VARCHAR(50), FALSE::BOOLEAN, 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32)),
        (6::INT, 6::INT, 'Voided trip'::VARCHAR(50), FALSE::BOOLEAN, 'VERIFIED'::VARCHAR(20), 'v1.0'::VARCHAR(32))
) AS t("PaymentTypeKey", "PaymentTypeCode", "PaymentTypeName", "IsElectronicTipObservable", "MappingStatus", "MetadataVersion")
ORDER BY "PaymentTypeKey"