{{ config(
    materialized='table',
    alias='DimDate',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"DateKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_PickupDateKey_fkey', 'PickupDateKey', 'DimDate', 'DateKey') }}",
        "{{ restore_fact_fk('FactTaxiTrip_DropoffDateKey_fkey', 'DropoffDateKey', 'DimDate', 'DateKey') }}"
    ]
) }}

WITH date_spine AS (
    SELECT generate_series(
        '{{ var("dq_date_domain_start", "2020-12-31") }}'::date,
        '{{ var("dq_date_domain_end", "2024-01-01") }}'::date,
        '1 day'::interval
    )::date AS full_date
),
unknown_member AS (
    SELECT
        0::INT AS "DateKey",
        '1900-01-01'::DATE AS "FullDate",
        1900::INT AS "YearNumber",
        '1900-00'::VARCHAR(7) AS "YearMonth",
        0::INT AS "MonthNumber",
        'Unknown'::VARCHAR(20) AS "MonthName",
        0::INT AS "DayOfWeek",
        'Unknown'::VARCHAR(20) AS "DayName",
        FALSE::BOOLEAN AS "IsWeekend"
),
valid_dates AS (
    SELECT
        TO_CHAR(full_date, 'YYYYMMDD')::INT AS "DateKey",
        full_date AS "FullDate",
        EXTRACT(YEAR FROM full_date)::INT AS "YearNumber",
        TO_CHAR(full_date, 'YYYY-MM')::VARCHAR(7) AS "YearMonth",
        EXTRACT(MONTH FROM full_date)::INT AS "MonthNumber",
        TRIM(TO_CHAR(full_date, 'Month'))::VARCHAR(20) AS "MonthName",
        EXTRACT(ISODOW FROM full_date)::INT AS "DayOfWeek",
        TRIM(TO_CHAR(full_date, 'Day'))::VARCHAR(20) AS "DayName",
        CASE WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS "IsWeekend"
    FROM date_spine
)
SELECT * FROM unknown_member
UNION ALL
SELECT * FROM valid_dates
ORDER BY "DateKey"