{# Shared B02/S02 policy: fixture mappings can only participate in mock runs. #}
{% macro mapping_is_active(alias) -%}
({{ alias }}.mapping_status = 'VERIFIED'
{% if var('b01_use_mock', false) %}
 or {{ alias }}.mapping_status = 'FIXTURE'
{% endif %})
{%- endmacro %}
