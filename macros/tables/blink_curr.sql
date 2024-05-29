{% macro default__blink_curr(rv_link, rv_esat, bv_curr_hubs, link_hash_key) -%}

{%- set exclude_cols=['last_updated', 'dv_source'] -%}

with

link as (
    select
        link.*,
        esat.is_deleted
    from {{ ref(rv_link) }} link
    inner join {{ ref(rv_esat) }} esat 
        on link.{{link_hash_key}} = esat.{{link_hash_key}}
    where  link.{{link_hash_key}} != {{ var('sdcvault.ghost_hk') }}::binary(16)
    qualify row_number() over (partition by link.{{link_hash_key}} order by esat.last_updated desc) = 1
),

blink as (
    select
        link.{{ link_hash_key }},
{%- for bhub in bv_curr_hubs %}

        {{bhub.bhub}}.{{ bhub.pk }},
    {% if var('sdcvault.natural_key', false) -%}
        {{bhub.bhub}}.{{ bhub.pk | lower | replace('hk_','nk_') }},
    {%- endif %}
    {% if var('sdcvault.integer_key', false) -%}
        {{bhub.bhub}}.{{ bhub.pk |lower | replace('hk_','sk_') }},
    {%- endif %}
        {{ dbt_utils.star(ref(bhub.hub), except=[bhub.pk]+exclude_cols, relation_alias=bhub.bhub) }},

    {%- for i in dbt_utils.get_filtered_columns_in_relation(ref(bhub.hub)) -%}
        {%- do exclude_cols.append(i) -%}
    {%- endfor -%}

{% endfor %}

    link.last_updated,
    link.dv_source

    from link 
    {% for bhub in bv_curr_hubs %}
    inner join {{ ref(bhub.bhub) }} {{bhub.bhub}}
        on link.{{bhub.pk}} = {{bhub.bhub}}.{{bhub.pk}}
    {%- endfor %}
    where not link.is_deleted
)

select * from blink

{%- endmacro %}