{#
  B01 (nbinh) - Ghi các dòng lỗi chuyển kiểu vào staging.quarantine (bảng T01).
  Idempotent: không ghi trùng cùng (source_file_sha256, source_row_number, quarantine_reason).
  prune_stale=true: xóa trước các dòng quarantine KHÔNG còn nằm trong stg_yellow_trips_rejects
  (vd: dòng bị đưa vào nhầm do lỗi rule cũ, nay đã ép kiểu được).
  Chạy: dbt run-operation load_quarantine [--args '{prune_stale: true}']
#}
{% macro load_quarantine(prune_stale=false) %}
    {% if prune_stale %}
        {% set prune_sql %}
            delete from staging.quarantine q
            where not exists (
                select 1 from {{ ref('stg_yellow_trips_rejects') }} r
                where r.source_file_sha256 = q.source_file_sha256
                  and r.source_row_number  = q.source_row_number
                  and r.quarantine_reason  = q.quarantine_reason
            )
        {% endset %}
        {% do run_query(prune_sql) %}
        {% do run_query('commit') %}
        {{ log('==> load_quarantine: da xoa cac dong quarantine khong con hop le (prune_stale)', info=True) }}
    {% endif %}
    {% set sql %}
        insert into staging.quarantine
            (source_file_name, source_file_sha256, source_row_number, load_batch_id,
             quarantine_reason, raw_payload)
        select r.source_file_name, r.source_file_sha256, r.source_row_number, r.load_batch_id,
               r.quarantine_reason, r.raw_payload
        from {{ ref('stg_yellow_trips_rejects') }} r
        where not exists (
            select 1 from staging.quarantine q
            where q.source_file_sha256 = r.source_file_sha256
              and q.source_row_number  = r.source_row_number
              and q.quarantine_reason  = r.quarantine_reason
        )
    {% endset %}
    {% set res = run_query(sql) %}
    {% do run_query('commit') %}
    {% set total = run_query('select count(*) from staging.quarantine') %}
    {{ log('==> load_quarantine: staging.quarantine hien co ' ~ total.columns[0].values()[0] ~ ' dong', info=True) }}
{% endmacro %}
