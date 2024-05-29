{%- macro default__msat(source_model, parent_hash_key, ma_hash_key, hash_diff_alias, src_payload,
                       src_ldts, src_rsrc, high_water_mark_bool, table_sample_prob, multi_batch_bool,
                       hash_diff_exclude, hash_diff_case_sensitive_bool) -%}

{%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set high_water_mark_bool = datavault4dbt.replace_standard(high_water_mark_bool, 'sdcvault.high_water_mark_bool', true) -%}
{%- set table_sample_prob = datavault4dbt.replace_standard(table_sample_prob, 'sdcvault.table_sample_prob', -1) -%}
{%- set multi_batch_bool = datavault4dbt.replace_standard(multi_batch_bool, 'sdcvault.multi_batch_bool', false) -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_ldts, src_rsrc, src_payload]) -%}
{%- set source_payload = datavault4dbt.expand_column_list(columns=[src_payload]) -%}
{%- set unique_hash_key = [parent_hash_key, ma_hash_key] -%}
{%- set source_relation = ref(source_model) -%}


with

{# selecting all source data, that is newer than latest data in msat if incremental #}
source_data as (

    select
        {{ datavault4dbt.print_list(unique_hash_key) }},
        {# Generate Hash Diff based on payload -#}
        {{ sdcvault.hash_diff(src_payload, alias=hash_diff_alias, is_case_sensitive=hash_diff_case_sensitive_bool, exclude=hash_diff_exclude) }},
        {{ datavault4dbt.print_list(source_cols) }}
    from {{ source_relation }}

{%- if table_sample_prob != -1 %}
    tablesample ({{ table_sample_prob }})
{% endif -%}

{%- if is_incremental() and high_water_mark_bool %}
    where {{ src_ldts }} > (
        select
            max({{ src_ldts }}) from {{ this }}
    )
{% endif %}
),

{# Get the latest record for each parent hashkey in existing msat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_msat as (

    select *
    from {{ this }}
    qualify row_number() over(partition by {{ datavault4dbt.print_list(unique_hash_key) }} order by {{ src_ldts }} desc) = 1  

),

deleted_records as (
    select
        {{ datavault4dbt.print_list(unique_hash_key) }},
        {{ hash_diff_alias }},
        current_timestamp() as {{ src_ldts }},
        {{ src_rsrc }},
        true as is_deleted,
        {{ datavault4dbt.print_list(source_payload) }}
    from latest_entries_in_msat msat 
    where not exists (
        select 1
        from source_data stg
        where {{ datavault4dbt.multikey(unique_hash_key, prefix=['msat','stg'], condition='=') }}
    )
        and not coalesce(msat.is_deleted, false)
),

{%- endif %}

{%- if multi_batch_bool %}
{#
    Deduplicate source by comparing each hash diff to the hash diff of the previous record, for each hash key.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_source_data as (

    select
    {{ datavault4dbt.print_list(unique_hash_key) }},
    {{ hash_diff_alias }},
    {{ datavault4dbt.print_list(source_cols) }}
    {%- if is_incremental() %},
    row_number() over(partition by {{ datavault4dbt.print_list(unique_hash_key) }} order by {{ src_ldts }}) as rn
    {%- endif %}
    from source_data
    qualify
        case
            when {{ hash_diff_alias }} = lag({{ hash_diff_alias }}) over(partition by {{ datavault4dbt.print_list(unique_hash_key) }} order by {{ src_ldts }}) then false
            else true
        end

),
{%- endif %}

{#
    select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the multi-active satellite.
#}
records_to_insert as (

    select
        {{ datavault4dbt.print_list(unique_hash_key) }},
        {{ hash_diff_alias }},
        {{ src_ldts }},
        {{ src_rsrc }},
        false as is_deleted,
        {{ datavault4dbt.print_list(source_payload) }}
    from {% if multi_batch_bool -%} deduplicated_ {%- endif -%} source_data src
{%- if is_incremental() %}
    where not exists (
        select 1
        from latest_entries_in_msat ltst
        where
            {{ datavault4dbt.multikey(parent_hash_key, prefix=['ltst', 'src'], condition='=') }}
            and (
                ltst.{{ src_ldts }} >= src.{{ src_ldts }}
                or (
                    {{ datavault4dbt.multikey(hash_diff_alias, prefix=['ltst', 'src'], condition='=') }}
                    and not ltst.is_deleted
                )
            )
    {% if multi_batch_bool -%}
            and src.rn = 1
    {%- endif %}
    )

    union all

    select *
    from deleted_records

{%- endif %}

)

select * from records_to_insert

{%- endmacro -%}