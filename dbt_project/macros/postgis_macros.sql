-- ============================================================================
-- TASK S02 (STINH): MACRO CHUYỂN ĐỔI HỆ TỌA ĐỘ POSTGIS & BẢO TOÀN FK DDS
-- ============================================================================

{% macro st_transform_multipolygon(geom_col, target_srid=4326, source_srid=2263) %}
    CASE
        WHEN {{ geom_col }} IS NULL THEN NULL
        WHEN ST_SRID({{ geom_col }}) = {{ target_srid }} THEN ST_Multi(ST_MakeValid({{ geom_col }}))::geometry(MultiPolygon, {{ target_srid }})
        WHEN ST_SRID({{ geom_col }}) = 0 THEN ST_Multi(ST_MakeValid(ST_Transform(ST_SetSRID({{ geom_col }}, {{ source_srid }}), {{ target_srid }})))::geometry(MultiPolygon, {{ target_srid }})
        ELSE ST_Multi(ST_MakeValid(ST_Transform({{ geom_col }}, {{ target_srid }})))::geometry(MultiPolygon, {{ target_srid }})
    END
{% endmacro %}

{% macro restore_fact_fk(fk_name, fact_col, dim_table, dim_pk) %}
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS {{ fk_name }};
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT {{ fk_name }}
        FOREIGN KEY ("{{ fact_col }}") REFERENCES dds."{{ dim_table }}" ("{{ dim_pk }}");
{% endmacro %}