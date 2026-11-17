{{ config(
    materialized='table',
    alias='DimRateCode',
    quoting={'identifier': true},
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (\"RateCodeKey\");",
        "{{ restore_fact_fk('FactTaxiTrip_RateCodeKey_fkey', 'RateCodeKey', 'DimRateCode', 'RateCodeKey') }}"
    ]
) }}

select 0::int as "RateCodeKey", 0::int as "RateCodeID",
       'Unknown / Unmapped Rate'::varchar(100) as "RateCodeName",
       'OTHER'::varchar(50) as "RateGroup", 'MISSING'::varchar(20) as "MappingStatus",
       '{{ var("dq_rule_version") }}'::varchar(32) as "MetadataVersion"
union all
select m.rate_code_key::int, m.rate_code_id::int, m.rate_code_name::varchar(100),
       upper(m.rate_group)::varchar(50), m.mapping_status::varchar(20), m.mapping_version::varchar(32)
from {{ ref('seed_rate_code_mapping') }} m
where {{ mapping_is_active('m') }}
