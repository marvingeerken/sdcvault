{#- 
    This macro creates Business Hubs, that show the current version. 
    A Business Hub joins the Effectivity Satellite to remove deleted Business Keys. 
-#}

{% macro default__bhub_curr(rv_hub, rv_esat, hash_key) -%}

with 

esat as (
    select 
        {{ hash_key }},
        is_deleted
    from {{ ref( rv_esat ) }}
    qualify row_number() over (partition by {{ hash_key }} order by last_updated desc) = 1
)

select 
    {{ hash_key }},
    {% if var('sdcvault.natural_key', false) -%}
    {{ sdcvault.natural_key(dbt_utils.get_filtered_columns_in_relation(ref(rv_hub), except=[hash_key, 'last_updated', 'dv_source'])) }} as {{hash_key|lower|replace('hk_','nk_')}},
    {%- endif %}
    {% if var('sdcvault.integer_key', false) -%}
    {{ sdcvault.integer_key(dbt_utils.get_filtered_columns_in_relation(ref(rv_hub), except=[hash_key, 'last_updated', 'dv_source'])) }} as {{hash_key|lower|replace('hk_','sk_')}},
    {%- endif %}
    {{ dbt_utils.star(ref(rv_hub), except=[hash_key] )}}
from {{ ref( rv_hub ) }} hub
inner join esat
    on hub.{{hash_key}} = esat.{{ hash_key }}
where not esat.is_deleted

{%- endmacro %}