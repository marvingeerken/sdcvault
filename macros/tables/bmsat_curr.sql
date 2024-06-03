{% macro default__bmsat_curr(bv_curr_parent, rv_ma_satellite, hash_key, ma_hash_key) -%}

{%- set ldts = var('sdcvault.ldts_alias', 'last_updated') -%}
{%- set rsrc = var('sdcvault.rsrc_alias', 'dv_source') -%}
{%- set dv_inserted = var('sdcvault.dv_inserted_alias', 'dv_inserted_at')-%}
{%- set exclude_columns = [hash_key, ldts, rsrc, dv_inserted, 'is_deleted', 'hd_' ~ rv_ma_satellite]  -%}

with

bv_parent as (

    select * exclude ({{ ldts }}, {{ rsrc }})
    from {{ ref(bv_curr_parent) }}

),

msat as (

    select *
    from {{ ref(rv_ma_satellite) }}
    where {{ hash_key }} != {{ var('sdcvault.ghost_hk') }}::binary(16)
    qualify row_number() over (partition by {{ hash_key }}, {{ ma_hash_key }} order by {{ ldts }} desc) = 1

),

final as (

    select 
        bv_parent.*,
        {{ dbt_utils.star(ref(rv_ma_satellite), except=exclude_columns, relation_alias='msat', quote_identifiers=false) | lower | indent(6) }},
        msat.last_updated,
        msat.dv_source
    from bv_parent
    inner join msat
        on bv_parent.{{ hash_key }} = msat.{{ hash_key }}
    where not msat.is_deleted

)

select * from final
{%- endmacro %}