{%- macro default__esat(source_models, parent_hash_key, src_ldts, src_rsrc) -%}

{%- set src_ldts = sdcvault.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = sdcvault.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set end_of_time = var('sdcvault.end_of_time', "'9999-12-31'") -%}

{%- if not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = [source_models] -%}
{%- endif -%}

{{- log('source_models: ' ~ source_models, false) -}}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hash_key, src_ldts, src_rsrc]) -%}

{%- if var('sdcvault.dv_inserted_bool', false) -%}
    {%- set dv_inserted = 'current_timestamp() as ' ~ var('sdcvault.dv_inserted_alias', 'dv_inserted_at') -%}
    {%- set final_columns_to_select = [parent_hash_key, src_ldts, dv_inserted, src_rsrc, 'start_date', 'end_date', 'is_deleted'] -%}
{%- else -%}
    {%- set final_columns_to_select = [parent_hash_key, src_ldts, src_rsrc]  -%}
{%- endif -%}


with

{% if is_incremental() %}
{#- Get all target records out of the existing esat for incremental logic. #}
distinct_target_records as (

    select *
    from {{ this }}
    qualify row_number() over (partition by {{ parent_hash_key }} order by {{ src_ldts }} desc) = 1

), 
{%- endif %}

{# Union all sources #}
source_union as (
    {% for src in source_models %}
    select {{ datavault4dbt.print_list(source_cols) }}
    from {{ ref(src) }}
        {% if not loop.last %}
    union all
        {% endif %}

    {%- endfor %}

),

{# Deduplicate the unionized records again to only insert the earliest one. -#}
earliest_hk_over_all_sources as (

    select *
    from source_union
    qualify row_number() over (partition by {{ parent_hash_key }} order by {{ src_ldts }}) = 1


),

{# Union keys that are either new, deleted or reappearing #}
insert_union as (

    {# Insert records from esat with is_deleted=false, if its not yet available -#}
    select {{ datavault4dbt.print_list(source_cols) }},
        {{ src_ldts }} as start_date,
        to_timestamp({{ end_of_time }}) as end_date,    
        false as is_deleted 
    from earliest_hk_over_all_sources

{%- if is_incremental() %}
    where {{ parent_hash_key }} not in (
        select {{ parent_hash_key }}
        from distinct_target_records
    )

    union all

    {# Insert records from esat with is_deleted=true, if they are not available in stage and not yet as deleted esat -#}
    select
        {{ parent_hash_key }},
        current_timestamp() as {{ src_ldts }},
        {{ src_rsrc }},
        esat.{{ src_ldts }} as start_date,
        current_timestamp() AS end_date,
        true as is_deleted
    from distinct_target_records esat
    where not is_deleted 
        and {{ parent_hash_key }} not in (
            select {{ parent_hash_key }}
            from earliest_hk_over_all_sources
        )

    union all

    {# Insert records from stage with is_deleted=false, if latest esat entry is is_deleted=true => HK is available again -#}
    select
        stg.{{ parent_hash_key }},

        {#- Use current_timestamp() as ldts for recurring keys with old ldts. Otherwise we would get Unique PK violation. #}
        case
            when stg.{{ src_ldts }} > esat.{{ src_ldts }} 
                then stg.{{ src_ldts }}
            else current_timestamp()
        end as {{ src_ldts }},

        stg.{{ src_rsrc }},
        stg.{{ src_ldts }} as start_date,
        to_timestamp({{ end_of_time }}) as end_date,
        false as is_deleted

    from earliest_hk_over_all_sources stg
    left join distinct_target_records esat
        on {{ datavault4dbt.multikey(parent_hash_key, prefix=['stg', 'esat'], condition='=') }}
    where esat.{{ parent_hash_key }} is not null
        and esat.is_deleted
{% endif %}
),


records_to_insert as (

    select {{ datavault4dbt.print_list(final_columns_to_select) }}
    from insert_union

)

select * from records_to_insert
{%- endmacro %}