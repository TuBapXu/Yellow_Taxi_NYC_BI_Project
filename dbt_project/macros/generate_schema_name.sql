{#
  B01 (nbinh): Dùng đúng tên schema khai báo trong dbt_project.yml (+schema: staging / dds)
  thay vì mặc định của dbt là "<target_schema>_<custom_schema>" (vd: staging_dds).
  Bảo đảm model rơi đúng vào schema staging / dds đã tạo ở T01.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
