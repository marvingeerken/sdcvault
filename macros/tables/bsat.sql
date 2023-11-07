{#- This macro creates Business Satellites. -#}

{% macro default__bsat(hash_key, hash_key_ma, bv_parent, sat, ldts) -%}

with 

bv_parent as (
    select * exclude (last_updated, dv_source)
    from {{ ref(bv_parent) }}
),

sat as (
    select *
    from {{ ref(sat) }}
    qualify row_number() over (partition by {{ hash_key }}{% if hash_key_ma %}, {{ hash_key_ma}}{% endif %} order by {{ ldts }} desc) = 1
)

select 
  bv_parent.*,
  {{ dbt_utils.star(ref(sat), except=[hash_key,'hd_'~sat,'last_updated','dv_source'], relation_alias='sat')}},
  sat.last_updated,
  sat.dv_source
from bv_parent
inner join sat
    on bv_parent.{{ hash_key }} = sat.{{ hash_key }}
where sat.{{ hash_key }} != {{var('sdcvault.ghost_hk')}}::binary(16)
{% if hash_key_ma %} and not sat.is_deleted {% endif %}


{%- endmacro %}
