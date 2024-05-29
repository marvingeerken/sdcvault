{%- macro default__esat(source_models, parent_hash_key, src_ldts, src_rsrc, high_water_mark_bool, table_sample_prob) -%}

{%- set end_of_time = var('sdcvault.end_of_time') -%}
{%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set high_water_mark_bool = datavault4dbt.replace_standard(high_water_mark_bool, 'sdcvault.high_water_mark_bool', true) -%}
{%- set limit_sources_num = datavault4dbt.replace_standard(limit_sources_num, 'sdcvault.limit_sources_num', -1) -%}
{%- set table_sample_prob = datavault4dbt.replace_standard(table_sample_prob, 'sdcvault.table_sample_prob', -1) -%}

{%- if limit_sources_num != -1 -%}
    {%- set source_models = source_models[:limit_sources_num] -%}
{%- endif -%}

{{- log('source_models'~source_models, false) -}}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hash_key, src_ldts, src_rsrc]) -%}


with

{% if not (source_models is iterable and source_models is not string) -%}
    {%- set source_models = [source_models] -%}
{%- endif -%}


{#- Get available HKs from stage -#}
src_union as (
    {% for source_model in source_models -%}
    select
        {{ datavault4dbt.print_list(source_cols) }}
    from {{ ref(source_model) }}

    {%- if table_sample_prob != -1 %}
    tablesample ({{ table_sample_prob }})
    {% endif -%}

    {%- if not loop.last %}
    union all
    {% endif -%}
    {% endfor -%}
),


{# Distinct HKs -#}
src_union_first as (
    select
        {{ datavault4dbt.print_list(source_cols) }}
    from src_union
    qualify row_number() over (partition by {{ parent_hash_key }} order by {{ src_ldts }}, {{ src_rsrc }}) = 1
),


{% if is_incremental() -%}
{# Get latest record per key from esat in incremental runs -#}
esat_latest as (
    select
        {{ datavault4dbt.print_list(source_cols) }}
    from {{ this }} sat
    qualify row_number() over (partition by {{ parent_hash_key }} order by {{ src_ldts }}) = 1
),
{%- endif %}


{# Prepare insert -#}
insert_rows as (

    {# Insert records from hub with is_deleted=false, if its not yet available -#}
    select
        {{ datavault4dbt.print_list(source_cols) }},
        {{ src_ldts }} as start_date,
        to_timestamp({{ end_of_time }}) AS end_date,    
        false as is_deleted 
    from src_union_first

{#- Following input matters on incremental runs only -#}
{%- if is_incremental() %}
    where {{ parent_hash_key }} not in (
        select {{ parent_hash_key }}
        from esat_latest sat
    )

    union all

    {# Insert records from hub with is_deleted=true, if they are not available in stage and not yet as deleted esat -#}
    select
        {{ parent_hash_key }},
        current_timestamp() as {{ src_ldts }},
        {{ src_rsrc }},
        esat_latest.{{ src_ldts }} as start_date,
        current_timestamp() AS end_date,
        true as is_deleted
    from esat_latest
    where not is_deleted 
        and {{ parent_hash_key }} not in (
            select {{ parent_hash_key }}
            from src_union_first
        )

    union all

    {# Insert records from stage with is_deleted=false, if latest esat entry is is_deleted=true => HK is available again -#}
    select
        stg.{{ parent_hash_key }},

        {# Use current_timestamp() as ldts for recurring keys with old ldts. Otherwise we would get Unique PK violation. -#}
        case
            when stg.{{ src_ldts }} > esat.{{ src_ldts }} 
                then stg.{{ src_ldts }},
            else current_timestamp()
        end as {{ src_ldts }},

        {{ src_rsrc }},
        stg.{{ src_ldts }} as start_date,
        to_timestamp({{ end_of_time }}) as end_date,
        false as is_deleted 
    from src_union_first stg
    left join esat_latest esat
        on {{ datavault4dbt.multikey(parent_hash_key, prefix=['stg', 'esat'], condition='=') }}
    where esat.parent_hash_key is not null
        and esat.is_deleted

{%- endif %}
)

select * from insert_rows

{%- endmacro %}