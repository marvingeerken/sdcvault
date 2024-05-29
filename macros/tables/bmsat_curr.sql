{#- This macro creates Business Satellites. -#}

{% macro default__bmsat_curr(bv_curr_parent, rv_satellite, hash_key, ma_hash_key) -%}

with

bv_parent as (
    select * exclude (last_updated, dv_source)
    from {{ ref(bv_curr_parent) }}
),

sat as (
    select *
    from {{ ref(rv_satellite) }}
    qualify row_number() over (partition by {{ hash_key }}, {{ ma_hash_key}} order by {{ ldts }} desc) = 1
)

select 
  bv_parent.*,
  {{ dbt_utils.star(ref(rv_satellite), except=[hash_key,'hd_'~sat,'last_updated','dv_source','is_deleted'], relation_alias='sat')}},
  sat.last_updated,
  sat.dv_source
from bv_parent
inner join sat
    on bv_parent.{{ hash_key }} = sat.{{ hash_key }}
where sat.{{ hash_key }} != {{var('sdcvault.ghost_hk')}}::binary(16)
{% if hash_key_ma %} and not sat.is_deleted {% endif %}


{%- endmacro %}
