{#
  B01 (nbinh) - Chuyển kiểu an toàn (TRY_CAST) tương thích PostgreSQL 16.
  Dùng pg_input_is_valid() (PG16+) nên KHÔNG BAO GIỜ ném lỗi giữa câu truy vấn.
  Quy ước Data Contract v1.2 mục 2.1:
    - NULL nguồn -> giữ NULL.
    - Giá trị có nhưng không chuyển được -> cast_error (đưa vào quarantine), KHÔNG âm thầm thành NULL.
  Mỗi kiểu có 2 macro: safe_* trả giá trị đã ép (hoặc NULL), *_error trả mã lỗi (hoặc NULL).
#}

{# Chuỗi rỗng / toàn khoảng trắng, và chuỗi chữ '\N' được coi như NULL nguồn.
   '\N': loader S01 (to_csv escapechar) ghi NULL thành chuỗi 2 ký tự backslash+N vào raw. #}
{% macro clean_text(col) -%}
    nullif(nullif(btrim({{ col }}), ''), E'\\N')
{%- endmacro %}

{# Chuẩn hóa mã nguyên lưu dạng số thực: '1.0' / '1.00' -> '1'; '1.5' giữ nguyên để báo lỗi #}
{% macro int_text(col) -%}
    regexp_replace({{ clean_text(col) }}, '^([+-]?[0-9]+)\.0+$', '\1')
{%- endmacro %}

{% macro safe_int(col) -%}
    case when pg_input_is_valid({{ int_text(col) }}, 'integer')
         then ({{ int_text(col) }})::integer end
{%- endmacro %}

{% macro int_error(col) -%}
    case
        when {{ clean_text(col) }} is null then null
        when pg_input_is_valid({{ int_text(col) }}, 'integer') then null
        when {{ clean_text(col) }} ~* '(nan|inf)' then 'NON_FINITE_NUMBER'
        when {{ clean_text(col) }} ~ '^[+-]?[0-9]+$' then 'OVERFLOW_ERROR'
        else 'TYPE_CAST_ERROR'
    end
{%- endmacro %}

{# Số thập phân: chặn NaN/Infinity (numeric của PG chấp nhận chúng) và tràn precision #}
{% macro safe_numeric(col, precision, scale) -%}
    case when {{ clean_text(col) }} !~* '(nan|inf)'
          and pg_input_is_valid({{ clean_text(col) }}, 'numeric({{ precision }},{{ scale }})')
         then ({{ clean_text(col) }})::numeric({{ precision }},{{ scale }}) end
{%- endmacro %}

{% macro numeric_error(col, precision, scale) -%}
    case
        when {{ clean_text(col) }} is null then null
        when {{ clean_text(col) }} ~* '(nan|inf)' then 'NON_FINITE_NUMBER'
        when pg_input_is_valid({{ clean_text(col) }}, 'numeric({{ precision }},{{ scale }})') then null
        when pg_input_is_valid({{ clean_text(col) }}, 'numeric') then 'OVERFLOW_ERROR'
        else 'TYPE_CAST_ERROR'
    end
{%- endmacro %}

{# Timestamp: chỉ nhận chuỗi bắt đầu bằng chữ số để loại các giá trị đặc biệt 'now', 'epoch', 'infinity' #}
{% macro safe_timestamp(col) -%}
    case when {{ clean_text(col) }} ~ '^[0-9]'
          and pg_input_is_valid({{ clean_text(col) }}, 'timestamp')
         then ({{ clean_text(col) }})::timestamp end
{%- endmacro %}

{% macro timestamp_error(col) -%}
    case
        when {{ clean_text(col) }} is null then null
        when {{ clean_text(col) }} ~ '^[0-9]'
         and pg_input_is_valid({{ clean_text(col) }}, 'timestamp') then null
        else 'UNPARSABLE_TIMESTAMP'
    end
{%- endmacro %}
