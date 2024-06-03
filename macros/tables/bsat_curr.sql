{% macro default__bsat_curr(bv_curr_parent, rv_satellite, hash_key) -%}

{%- set ldts = var('sdcvault.ldts_alias', 'last_updated') -%}
{%- set rsrc = var('sdcvault.rsrc_alias', 'dv_source') -%}
{%- set dv_inserted = var('sdcvault.dv_inserted_alias', 'dv_inserted_at')-%}
{%- set exclude_columns = [hash_key, ldts, rsrc, dv_inserted, 'hd_' ~ rv_satellite]  -%}

with

bv_parent as (

    select * exclude ({{ ldts }}, {{ rsrc }})
    from {{ ref(bv_curr_parent) }}

),

sat as (

    select *
    from {{ ref(rv_satellite) }}
    where {{ hash_key }} != {{ var('sdcvault.ghost_hk') }}::binary(16)
    qualify row_number() over (partition by {{ hash_key }} order by {{ ldts }} desc) = 1

),

final as (

    select 
        bv_parent.*,
        {{ dbt_utils.star(ref(rv_satellite), except=exclude_columns, relation_alias='sat', quote_identifiers=false) | lower | indent(6) }},
        sat.last_updated,
        sat.dv_source
    from bv_parent
    inner join sat
        on bv_parent.{{ hash_key }} = sat.{{ hash_key }}

)

select * from final
{%- endmacro %}