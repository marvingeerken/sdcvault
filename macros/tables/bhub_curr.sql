{%- macro default__bhub_curr(rv_hub, rv_esat, hash_key) %}

{%- set ldts = var('sdcvault.ldts_alias', 'last_updated') -%}
{%- set rsrc = var('sdcvault.rsrc_alias', 'dv_source') -%}
{%- set dv_inserted = var('sdcvault.dv_inserted_alias', 'dv_inserted_at')-%}
{%- set limit_sources = var('sdcvault.limit_sources', -1) | int-%}
{%- set table_sample = var('sdcvault.table_sample', -1) | int -%}
{%- set exclude_columns = [hash_key, ldts, rsrc, dv_inserted]  -%}

with 

{% if limit_sources == -1 and table_sample == -1 %}
esat as (

    select 
        {{ hash_key }},
        is_deleted
    from {{ ref( rv_esat ) }}
    where {{ hash_key }} != {{ var('sdcvault.ghost_hk') }}::binary(16)
    qualify row_number() over (partition by {{ hash_key }} order by {{ ldts }} desc) = 1

),
{% endif %}

final as (

    select 
        hub.{{ hash_key }},
{%- if var('sdcvault.natural_key', false) %}
        {{ sdcvault.natural_key(dbt_utils.get_filtered_columns_in_relation(ref(rv_hub), except=exclude_columns)) | lower }} as {{hash_key | lower | replace('hk_','nk_') }},
{%- endif %}
{%- if var('sdcvault.integer_key', false) %}
        {{ sdcvault.integer_key(dbt_utils.get_filtered_columns_in_relation(ref(rv_hub), except=exclude_columns)) | lower }} as {{ hash_key | lower | replace('hk_','sk_') }},
{%- endif %}

        {{ dbt_utils.star(ref(rv_hub), except=[hash_key, dv_inserted], relation_alias='hub', quote_identifiers=false) | lower | indent(6) }}

    from {{ ref( rv_hub ) }} hub
{% if limit_sources == -1 and table_sample == -1 %}
    inner join esat
        on hub.{{hash_key}} = esat.{{ hash_key }}
    where not esat.is_deleted
{% endif %}
)


select * from final
{%- endmacro %}