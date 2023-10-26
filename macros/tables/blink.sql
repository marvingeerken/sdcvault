{#- 
    This macro creates Business Links. 
    A Business Link joins the Effectivity Satellite to remove deleted relationships.
    It also joins the Business Hub to remove relationships, that hold deleted Business Keys.
-#}

{% macro default__blink(hash_key_link, esat, link, bhubs) -%}

{%- set exclude_cols=['last_updated', 'dv_source'] -%}

with

link as (
    select
        link.*,
        esat.deleted
    from {{ ref(link) }} link
    inner join {{ ref(esat) }} esat 
        on link.{{hash_key_link}} = esat.{{hash_key_link}}
    where  link.{{hash_key_link}} != {{ var('sdcvault.ghost_hk') }}::binary(16)
    qualify row_number() over (partition by link.{{hash_key_link}} order by esat.last_updated desc) = 1
),

blink as (
    select
    link.{{ hash_key_link }},
    {%- for bhub in bhubs %}

    {{bhub.bhub}}.{{ bhub.pk }},
    {{bhub.bhub}}.{{ bhub.pk|lower|replace('hk_','nk_') }},
    {% set hub = modules.re.sub('^b|_curr$', '', bhub.bhub) -%}

    {{ dbt_utils.star(ref(hub), except=[bhub.pk]+exclude_cols, relation_alias=bhub.bhub) }},

    {%- for i in dbt_utils.get_filtered_columns_in_relation(ref(hub)) -%}
        {%- do exclude_cols.append(i) -%}
    {%- endfor -%}

    {% endfor %}

    link.last_updated,
    link.dv_source

    from link 
    {% for bhub in bhubs %}
    inner join {{ ref(bhub.bhub) }} {{bhub.bhub}}
        on link.{{bhub.pk}} = {{bhub.bhub}}.{{bhub.pk}}
    {%- endfor %}
    where not deleted
)

select * from blink


{%- endmacro %}