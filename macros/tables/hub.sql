{%- macro default__hub(source_models, hash_key, business_key, src_ldts, src_rsrc, high_water_mark) -%}

{%- set src_ldts = sdcvault.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = sdcvault.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set high_water_mark = sdcvault.replace_standard(high_water_mark, 'sdcvault.high_water_mark', true) -%}
{%- set limit_sources = var('sdcvault.limit_sources', -1) | int -%}
{%- set table_sample = var('sdcvault.table_sample', -1) | int -%}

{%- if datavault4dbt.is_list(source_models) and limit_sources != -1 and (source_models | length) > limit_sources + 1 -%}
    {%- set included_sources = source_models[:limit_sources] + source_models[-1:] -%}
/*
  Excluded sources on source limit:
    {%- for source in source_models if source not in included_sources %}
    {{ ref(source.name) }}
    {%- endfor %}
*/
    {% set source_models = included_sources %}
{% endif %}

{{- log('source_models' ~ source_models, false) -}}

{%- set ns = namespace(last_cte= "", source_included_before = {}, has_rsrc_static_defined=true, source_models_rsrc_dict={}) -%}

{#- Select the Business Key column from the first source model definition provided in the hub model and put them in an array. -#}
{%- set business_key = datavault4dbt.expand_column_list(columns=[business_key]) -%}

{#- If no specific bk_columns is defined for each source, we apply the values set in the business_key variable. -#}
{#- If no specific hk_column is defined for each source, we apply the values set in the hash_key variable. -#}
{#- If no rsrc_static parameter is defined in ANY of the source models then the whole code block of record_source performance lookup is not executed -#}
{#- For the use of record_source performance lookup it is required that every source model has the parameter rsrc_static defined and it cannot be an empty string -#}
{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'hk_column':hash_key}, business_keys=business_key)) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}

{%- if var('sdcvault.dv_inserted_bool', false) -%}
    {%- set dv_inserted = 'current_timestamp() as ' ~ var('sdcvault.dv_inserted_alias', 'dv_inserted_at') -%}
    {%- set final_columns_to_select = [hash_key] + business_key + [src_ldts, dv_inserted, src_rsrc] -%}
{%- else -%}
    {%- set final_columns_to_select = [hash_key] + business_key + [src_ldts, src_rsrc] -%}
{%- endif -%}


with

{% if is_incremental() %}
{#- Get all target hash keys out of the existing hub for incremental logic. #}
distinct_target_hash_keys as (

    select {{ hash_key }}
    from {{ this }}

),
    {%- if ns.has_rsrc_static_defined and high_water_mark -%}
        {% for source_model in source_models %}
         {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
            {%- set source_number = source_model.id | string -%}
            {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number] -%}

            {{- log('rsrc_statics: '~ rsrc_statics, false) }}

            {%- set rsrc_static_query_source -%}
                select count(*) from (
                {%- for rsrc_static in rsrc_statics -%}
                    select {{ src_rsrc }}
                    from {{ this }}
                    where {{ src_rsrc }} ilike '{{ rsrc_static }}'
                    {%- if not loop.last %}
                        union all
                    {% endif -%}
                {%- endfor -%}
                )
            {%- endset %}

            {{- log('rsrc static query: ' ~ rsrc_static_query_source, false) }}

rsrc_static_{{ source_number }} as (
            {% for rsrc_static in rsrc_statics %}
    select 
        *,
        '{{ rsrc_static }}' as rsrc_static
    from {{ this }}
    where {{ src_rsrc }} like '{{ rsrc_static }}'
                {%- if not loop.last %}
        union all
                {% endif -%}
            {%- endfor %}
            {% set ns.last_cte = "rsrc_static_{}".format(source_number) %}
),
            
            {%- set source_in_target = true -%}
            
            {%- if execute -%}
                {%- set rsrc_static_result = run_query(rsrc_static_query_source) -%}
                {%- set row_count = rsrc_static_result.columns[0].values()[0] -%}

                {{ log('row_count for ' ~ source_model ~ ' is ' ~ row_count, false) }}

                {%- if row_count == 0 -%}
                    {%- set source_in_target = false -%}
                {%- endif -%}
            {%- endif -%}

            {%- do ns.source_included_before.update({source_model.id: source_in_target}) -%}
        {% endfor %}

        {%- if source_models | length > 1 %}

rsrc_static_union as (
            {#- Create one unionized table over all sources. It will be the same as the already existing
                hub, but extended by the rsrc_static column. #}
            {%- for source_model in source_models %}
                {%- set source_number = source_model.id | string %}

    select * from rsrc_static_{{ source_number }}
                {% if not loop.last %}
    union all
                {%- endif %}
            {%- endfor %}
            {%- set ns.last_cte = "rsrc_static_union" %}
),
        {% endif %}

max_ldts_per_rsrc_static_in_target as (
    {# Use the previously created CTE to calculate the max load date timestamp per rsrc_static. #}
    select
        rsrc_static,
        max({{ src_ldts }}) as max_ldts
    from {{ ns.last_cte }}
    group by rsrc_static

),
    {%- endif %}
{% endif -%}

{% for source_model in source_models %}

    {%- set source_number = source_model.id | string -%}

    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number|string] -%}
    {%- endif -%}

    {%- if 'hk_column' not in source_model.keys() %}
        {%- set hk_column = hash_key -%}
    {%- else -%}
        {%- set hk_column = source_model['hk_column'] -%}
    {% endif %}

src_new_{{ source_number }} as (

    select
        src.{{ hk_column }} as {{ hash_key }},
        {%- for bk in source_model['bk_columns'] %}
        src.{{ bk }},
        {%- endfor %}
        src.{{ src_ldts }},
        src.{{ src_rsrc }}
    from {{ ref(source_model.name) }} src
    {{- log('rsrc_statics defined?: ' ~ ns.source_models_rsrc_dict[source_number | string], false) -}}

    {%- if table_sample != -1 %}
    tablesample ({{ table_sample }})
    {%- endif %}

    {%- if is_incremental() and ns.has_rsrc_static_defined and ns.source_included_before[source_number | int] and high_water_mark %}
    inner join max_ldts_per_rsrc_static_in_target maxl
        on
        {%- for rsrc_static in rsrc_statics %}
            maxl.rsrc_static = '{{ rsrc_static }}'
            {%- if not loop.last -%} or
            {% endif -%}
        {%- endfor %}
    where src.{{ src_ldts }} > maxl.max_ldts
    {%- elif is_incremental() and source_models | length == 1 and not ns.has_rsrc_static_defined and high_water_mark %}
    where src.{{ src_ldts }} > (
        select max({{ src_ldts }})
        from {{ this }}
    )
    {%- endif -%}

    {%- set ns.last_cte = "src_new_{}".format(source_number) %}

),
{%- endfor %}

{% if source_models | length > 1 %}
source_new_union as (

    {%- for source_model in source_models -%}

        {% set source_number = source_model.id | string %}

    select
        {{ hash_key }},
        {%- for bk in source_model['bk_columns'] %}
        {{ bk }} as {{ business_key[loop.index - 1] }},
        {%- endfor %}
        {{ src_ldts }},
        {{ src_rsrc }}
    from src_new_{{ source_number }}
        {% if not loop.last %}
    union all
        {%- endif -%}

    {%- endfor %}

    {%- set ns.last_cte = 'source_new_union' %}
),
{% endif %}

earliest_hk_over_all_sources as (

    {# Deduplicate the unionized records again to only insert the earliest one. -#}
    select *
    from {{ ns.last_cte }}
    qualify row_number() over (partition by {{ hash_key }} order by {{ src_ldts }}) = 1

{%- set ns.last_cte = 'earliest_hk_over_all_sources' %}

),


records_to_insert as (
    {# select everything from the previous CTE, if incremental filter for hash keys that are not already in the hub. #}
    select {{ datavault4dbt.print_list(final_columns_to_select) }}
    from {{ ns.last_cte }}

{%- if is_incremental() %}
    where {{ hash_key }} not in (
        select * from distinct_target_hash_keys
    )
{% endif %}
)

select * from records_to_insert
{%- endmacro %}