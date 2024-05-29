{%- macro default__link(source_models, link_hash_key, foreign_hash_keys, src_ldts, src_rsrc,
                        high_water_mark_bool, limit_sources_num, table_sample_prob) -%}

{%- set src_ldts = sdcvault.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = sdcvault.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set high_water_mark_bool = sdcvault.replace_standard(high_water_mark_bool, 'sdcvault.high_water_mark_bool', true) -%}
{%- set limit_sources_num = sdcvault.replace_standard(limit_sources_num, 'sdcvault.limit_sources_num', -1) -%}
{%- set table_sample_prob = sdcvault.replace_standard(table_sample_prob, 'sdcvault.table_sample_prob', -1) -%}

{%- if datavault4dbt.is_list(source_models) and limit_sources_num != -1 -%}
    {%- set source_models = source_models[:limit_sources_num] -%}
{%- endif -%}

{%- if not (foreign_hash_keys is iterable and foreign_hash_keys is not string) -%}
    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Only one foreign key provided for this link. At least two required.") }}
    {%- endif %}
{%- endif -%}

{%- set ns = namespace(last_cte= "", source_included_before = {}, has_rsrc_static_defined=true, source_models_rsrc_dict={}) -%}

{#- If no specific link_hk and fk_columns are defined for each source, we apply the values set in the link_hash_key and foreign_hash_keys variable. #}
{#- If no rsrc_static parameter is defined in ANY of the source models then the whole code block of record_source performance lookup is not executed  #}
{#- For the use of record_source performance lookup it is required that every source model has the parameter rsrc_static defined and it cannot be an empty string #}
{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'link_hk':link_hash_key}, foreign_hashkeys=foreign_hash_keys)) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}

{{- log('source_models: '~  source_models, false) -}}

{%- set final_columns_to_select = [link_hash_key] + foreign_hash_keys + [src_ldts] + [src_rsrc] -%}


with

{% if is_incremental() %}
{#- Get all link hash keys out of the existing link for later incremental logic. #}
distinct_target_hash_keys as (
        
    select {{ link_hash_key }}
    from {{ this }}

),
    {%- if ns.has_rsrc_static_defined and high_water_mark_bool -%}
        {% for source_model in source_models %}
        {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
            {%- set source_number = source_model.id | string -%}
            {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number] -%}

            {{- log('rsrc_statics: '~ rsrc_statics, false) }}

            {%- set rsrc_static_query_source -%}
                select count(*) from (
                {%- for rsrc_static in rsrc_statics %}
                    select {{ src_rsrc }},
                    '{{ rsrc_static }}' as rsrc_static
                    from {{ this }}
                    where {{ src_rsrc }} like '{{ rsrc_static }}'
                    {%- if not loop.last %}
                        union all
                    {% endif -%}
                {%- endfor -%}
                )
            {%- endset %}

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

                {{ log('row_count for '~source_model~' is '~row_count, false) }}

                {%- if row_count == 0 -%}
                    {%- set source_in_target = false -%}
                {%- endif -%}
            {%- endif -%}

            {%- do ns.source_included_before.update({source_model.id: source_in_target}) -%}
        {% endfor %}
        {% if source_models | length > 1 %}

rsrc_static_union as (
            {#- Create one unionized table over all sources. It will be the same as the already existing
               link, but extended by the rsrc_static column. #}
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
{#- Select all deduplicated records from each source, and filter for records that are newer
   than the max ldts inside the existing link, if incremental. #}

    {%- set source_number = source_model.id | string -%}

    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number|string] -%}
    {%- endif -%}

    {%- if 'link_hk' not in source_model.keys() %}
        {%- set link_hk = link_hash_key -%}
    {%- else -%}
        {%- set link_hk = source_model['link_hk'] -%}
    {% endif %}

src_new_{{ source_number }} as (

    select
        src.{{ link_hk }} as {{ link_hash_key }},
        {%- for fk in source_model['fk_columns'] %}
        src.{{ fk }},
        {%- endfor %}
        src.{{ src_ldts }},
        src.{{ src_rsrc }}
    from {{ ref(source_model.name) }} src
    {{- log('rsrc_statics defined?: ' ~ ns.source_models_rsrc_dict[source_number|string], false) -}}

    {%- if table_sample_prob != -1 %}
    tablesample ({{ table_sample_prob }})
    {%- endif %}

    {%- if is_incremental() and ns.has_rsrc_static_defined and ns.source_included_before[source_number|int] and high_water_mark_bool %}
    inner join max_ldts_per_rsrc_static_in_target maxl
        on
        {%- for rsrc_static in rsrc_statics %}
            maxl.rsrc_static = '{{ rsrc_static }}'
            {%- if not loop.last -%} or
            {% endif -%}
        {%- endfor %}
    where src.{{ src_ldts }} > maxl.max_ldts
    {%- elif is_incremental() and source_models | length == 1 and not ns.has_rsrc_static_defined and not high_water_mark_bool %}
    where src.{{ src_ldts }} > (
        select max({{ src_ldts }})
        from {{ this }}
    )
    {%- endif %}

    {%- set ns.last_cte = "src_new_{}".format(source_number) %}

),
{%- endfor %}

{% if source_models | length > 1 %}
source_new_union as (
{#- Unionize the new records from all sources. #}

    {%- for source_model in source_models -%}

        {% set source_number = source_model.id | string %}

    select
        {{ link_hash_key }},

        {%- for fk in source_model['fk_columns'] | list %}
        {{ fk }} as {{ foreign_hash_keys[loop.index - 1] }},
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
    qualify row_number() over (partition by {{ link_hash_key }} order by {{ src_ldts }}) = 1

{%- set ns.last_cte = 'earliest_hk_over_all_sources' %}

),


records_to_insert as (
    {# select everything from the previous CTE, if incremental filter for hash keys that are not already in the link. #}
    select {{ datavault4dbt.print_list(final_columns_to_select) }}
    from {{ ns.last_cte }}

{%- if is_incremental() %}
    where {{ link_hash_key }} not in (
        select * from distinct_target_hash_keys
    )
{% endif %}
)

select * from records_to_insert
{%- endmacro %}