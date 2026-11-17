{{ config(
    materialized='table',
    alias='DimTime',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"TimeKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_PickupTimeKey_fkey', 'PickupTimeKey', 'DimTime', 'TimeKey') }}",
        "{{ restore_fact_fk('FactTaxiTrip_DropoffTimeKey_fkey', 'DropoffTimeKey', 'DimTime', 'TimeKey') }}"
    ]
) }}

WITH minute_spine AS (
    SELECT generate_series(0, 1439) AS minute_of_day
),
unknown_member AS (
    SELECT
        0::INT AS "TimeKey",
        '00:00'::VARCHAR(5) AS "TimeLabel",
        -1::INT AS "HourNumber",
        'Unknown'::VARCHAR(10) AS "TimeBand"
),
valid_times AS (
    SELECT
        (minute_of_day + 1)::INT AS "TimeKey",
        LPAD((minute_of_day / 60)::TEXT, 2, '0') || ':' || LPAD((minute_of_day % 60)::TEXT, 2, '0') AS "TimeLabel",
        (minute_of_day / 60)::INT AS "HourNumber",
        CASE
            WHEN (minute_of_day / 60) BETWEEN 0 AND 5 THEN '00-05'
            WHEN (minute_of_day / 60) BETWEEN 6 AND 11 THEN '06-11'
            WHEN (minute_of_day / 60) BETWEEN 12 AND 17 THEN '12-17'
            ELSE '18-23'
        END::VARCHAR(10) AS "TimeBand"
    FROM minute_spine
)
SELECT * FROM unknown_member
UNION ALL
SELECT * FROM valid_times
ORDER BY "TimeKey"