{{ config(
    materialized='table',
    alias='DimZone',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"ZoneKey\");",
        "CREATE INDEX IF NOT EXISTS idx_dimzone_geometry ON {{ this }} USING GIST (\"ZoneGeometry\");",
        "{{ restore_fact_fk('FactTaxiTrip_PickupZoneKey_fkey', 'PickupZoneKey', 'DimZone', 'ZoneKey') }}",
        "{{ restore_fact_fk('FactTaxiTrip_DropoffZoneKey_fkey', 'DropoffZoneKey', 'DimZone', 'ZoneKey') }}"
    ]
) }}

WITH unknown_member AS (
    SELECT
        0::INT AS "ZoneKey",
        0::INT AS "LocationID",
        'Unknown Zone'::VARCHAR(100) AS "ZoneName",
        'Unknown'::VARCHAR(50) AS "BoroughName",
        'Unknown'::VARCHAR(50) AS "ServiceZone",
        NULL::geometry(MultiPolygon, 4326) AS "ZoneGeometry",
        FALSE::BOOLEAN AS "HasGeometry",
        'MISSING'::VARCHAR(20) AS "MappingStatus",
        'v1.0'::VARCHAR(32) AS "MetadataVersion"
),
joined_zones AS (
    SELECT
        l.location_id::INT AS "ZoneKey",
        l.location_id::INT AS "LocationID",
        COALESCE(l.zone_name, 'Unknown')::VARCHAR(100) AS "ZoneName",
        COALESCE(l.borough, 'Unknown')::VARCHAR(50) AS "BoroughName",
        COALESCE(l.service_zone, 'Unknown')::VARCHAR(50) AS "ServiceZone",
        CASE
            WHEN l.location_id IN (264, 265) OR s.geom_4326 IS NULL OR NOT s.is_valid_geom THEN NULL::geometry(MultiPolygon, 4326)
            ELSE {{ st_transform_multipolygon('s.geom_4326', 4326, 2263) }}
        END AS "ZoneGeometry",
        CASE
            WHEN l.location_id IN (264, 265) OR s.geom_4326 IS NULL OR NOT s.is_valid_geom THEN FALSE
            ELSE TRUE
        END::BOOLEAN AS "HasGeometry",
        'VERIFIED'::VARCHAR(20) AS "MappingStatus",
        'v1.0'::VARCHAR(32) AS "MetadataVersion"
    FROM staging.raw_taxi_zone_lookup l
    LEFT JOIN staging.raw_taxi_zones_shp s
        ON l.location_id = s.location_id
)
SELECT * FROM unknown_member
UNION ALL
SELECT * FROM joined_zones
ORDER BY "ZoneKey"