{#
  B01 (nbinh) - RFC-002 v2, điều kiện 5: thay thế quarantine THEO TỪNG FILE cho PA3 (Worker W5).
  - Chỉ xóa và ghi lại các dòng quarantine của đúng file đang xử lý; quarantine của các tháng khác giữ nguyên.
  - KHÔNG dùng load_quarantine(prune_stale=true) trong PA3: staging chỉ chứa 1 file nên prune_stale
    sẽ xóa sạch quarantine của mọi tháng còn lại.
  - DELETE + INSERT chạy trong cùng một transaction: lỗi giữa chừng thì quarantine cũ của file vẫn còn.

  Dùng từ dbt:
    dbt run-operation replace_quarantine_for_file --args '{source_file_name: yellow_tripdata_2024-01.parquet}'
  Dùng trong transaction W5 của TTU (psycopg): lấy câu SQL từ macro replace_quarantine_for_file_sql()
    (dbt compile --inline "{{ replace_quarantine_for_file_sql('<file>') }}") hoặc chép nguyên 2 câu lệnh bên dưới,
    đặt chung transaction với DELETE/INSERT Fact.
#}
{% macro replace_quarantine_for_file_sql(source_file_name) -%}
    {%- set f = source_file_name | replace("'", "''") -%}
    delete from staging.quarantine
    where source_file_name = '{{ f }}';

    insert into staging.quarantine
        (source_file_name, source_file_sha256, source_row_number, load_batch_id,
         quarantine_reason, raw_payload)
    select r.source_file_name, r.source_file_sha256, r.source_row_number, r.load_batch_id,
           r.quarantine_reason, r.raw_payload
    from {{ ref('stg_yellow_trips_rejects') }} r
    where r.source_file_name = '{{ f }}';
{%- endmacro %}

{% macro replace_quarantine_for_file(source_file_name=none) %}
    {% if source_file_name is none or (source_file_name | trim) == '' %}
        {{ exceptions.raise_compiler_error('replace_quarantine_for_file: bắt buộc truyền source_file_name') }}
    {% endif %}
    {% do run_query('begin;\n' ~ replace_quarantine_for_file_sql(source_file_name) ~ '\ncommit;') %}
    {% set f = source_file_name | replace("'", "''") %}
    {% set cnt = run_query("select count(*) from staging.quarantine where source_file_name = '" ~ f ~ "'") %}
    {{ log('==> replace_quarantine_for_file: ' ~ source_file_name ~ ' hien co ' ~ cnt.columns[0].values()[0] ~ ' dong quarantine', info=True) }}
{% endmacro %}
