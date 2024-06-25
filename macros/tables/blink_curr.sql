{#- 
    This macro creates Business Links. 
    A Business Link joins the Effectivity Satellite to remove deleted relationships.
    It also joins the Business Hub to remove relationships, that hold deleted Business Keys.
-#}

{% macro default__blink_curr(rv_link, rv_esat, bv_curr_hubs, link_hash_key) -%}

{%- set ldts = var('sdcvault.ldts_alias', 'last_updated') -%}
{%- set rsrc = var('sdcvault.rsrc_alias', 'dv_source') -%}
{%- set dv_inserted = var('sdcvault.dv_inserted_alias', 'dv_inserted_at') -%}
{%- set limit_sources = var('sdcvault.limit_sources', -1) | int-%}
{%- set table_sample = var('sdcvault.table_sample', -1) | int -%}
{%- set exclude_columns = [hash_key, ldts, rsrc, dv_inserted]  -%}

with

{% if limit_sources == -1 and table_sample == -1 %}
esat as (

    select 
        {{ link_hash_key }},
        is_deleted
    from {{ ref( rv_esat ) }}
    qualify row_number() over (partition by {{ link_hash_key }} order by {{ ldts }} desc) = 1

),
{%- else %}
-- Excluded on dev limit: {{ ref( rv_esat ) }}
{% endif %}

link as (

    select link.*
    from {{ ref(rv_link) }} link
{%- if limit_sources == -1 and table_sample == -1 %}
    inner join esat
        on link.{{ link_hash_key }} = esat.{{ link_hash_key }}
    where not esat.is_deleted
{% endif %}
),


blink as (

    select
        link.{{ link_hash_key }},

{%- for bhub in bv_curr_hubs %}
        {{bhub.name}}.{{ bhub.hk }},
    {%- if var('sdcvault.natural_key', false) %}
        {{bhub.name}}.{{ bhub.hk | lower | replace('hk_','nk_') }},
    {%- endif %}
    {%- if var('sdcvault.integer_key', false) %}
        {{bhub.name}}.{{ bhub.hk |lower | replace('hk_','sk_') }},
    {%- endif %}
        {%- if not bhub.rv_name %}
            {%- set rv_name = bhub.name | replace('bhub', 'hub') | replace('_curr', '') %}
        {%- endif %}
        {{ dbt_utils.star(ref(rv_name), except=[bhub.hk]+exclude_columns, relation_alias=bhub.name, quote_identifiers=false) | lower | indent(6) }},

    {%- for i in dbt_utils.get_filtered_columns_in_relation(ref(rv_name)) -%}
        {%- do exclude_columns.append(i) -%}
    {%- endfor -%}

{% endfor %}

        link.{{ ldts }},
        link.{{ rsrc }}

    from link 
    {% for bhub in bv_curr_hubs %}
    inner join {{ ref(bhub.name) }} {{bhub.name}}
        on link.{{bhub.hk}} = {{bhub.name}}.{{bhub.hk}}
    {%- endfor %}

)


select * from blink
{%- endmacro %}