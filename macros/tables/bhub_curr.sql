{%- macro default__bhub_curr(rv_hub, rv_esat, hash_key) %}

{%- set src_ldts = sdcvault.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = sdcvault.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set exclude_columns = [hash_key, src_ldts, src_rsrc] -%}

with 

esat as (

    select 
        {{ hash_key }},
        is_deleted
    from {{ ref( rv_esat ) }}
    qualify row_number() over (partition by {{ hash_key }} order by {{ src_ldts }} desc) = 1

),


final as (

    select 
        hub.{{ hash_key }},
{%- if var('sdcvault.natural_key', false) %}
        {{ sdcvault.natural_key(dbt_utils.get_filtered_columns_in_relation(ref(rv_hub), except=exclude_columns)) | lower }} as {{hash_key | lower | replace('hk_','nk_') }},
{%- endif %}
{%- if var('sdcvault.integer_key', false) %}
        {{ sdcvault.integer_key(dbt_utils.get_filtered_columns_in_relation(ref(rv_hub), except=exclude_columns)) | lower }} as {{ hash_key | lower | replace('hk_','sk_') }},
{%- endif %}

        {{ dbt_utils.star(ref(rv_hub), except=[hash_key], relation_alias='hub', quote_identifiers=false) | lower | indent(6) }}

    from {{ ref( rv_hub ) }} hub
    inner join esat
        on hub.{{hash_key}} = esat.{{ hash_key }}
    where not esat.is_deleted

)


select * from final
{%- endmacro %}