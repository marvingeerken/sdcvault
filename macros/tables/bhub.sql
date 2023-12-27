{#- 
    This macro creates Business Hubs, that show the current version. 
    A Business Hub joins the Effectivity Satellite to remove deleted Business Keys. 
-#}

{% macro default__bhub(hash_key, hub, esat) -%}

with esat as (
    select 
    {{ hash_key }} as esat_hk,
    is_deleted
    from {{ ref( esat ) }}
    qualify row_number() over (partition by {{ hash_key }} order by last_updated desc) = 1
)

select 
    {{ hash_key }},
    {% if var('sdcvault.natural_key') -%}
    {{ sdcvault.natural_key(dbt_utils.get_filtered_columns_in_relation(ref(hub), except=[hash_key, 'last_updated', 'dv_source'])) }} as {{hash_key|lower|replace('hk_','nk_')}},
    {%- endif %}
    {% if var('sdcvault.integer_key') -%}
    {{ sdcvault.integer_key(dbt_utils.get_filtered_columns_in_relation(ref(hub), except=[hash_key, 'last_updated', 'dv_source'])) }} as {{hash_key|lower|replace('hk_','sk_')}},
    {%- endif %}
    {{ dbt_utils.star(ref(hub), except=[hash_key] )}}
from {{ ref( hub ) }}  h
inner join esat
    on h.{{hash_key}} = esat_hk
where not esat.is_deleted

{%- endmacro %}