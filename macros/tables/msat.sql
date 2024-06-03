{%- macro default__msat(source_model, parent_hash_key, ma_hash_key, hash_diff_alias, src_payload, src_ldts, src_rsrc,
                        table_sample_prob, hash_diff_exclude, hash_diff_case_sensitive_bool) -%}

{%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set table_sample_prob = datavault4dbt.replace_standard(table_sample_prob, 'sdcvault.table_sample_prob', -1) -%}
{%- set multi_batch_bool = datavault4dbt.replace_standard(multi_batch_bool, 'sdcvault.multi_batch_bool', false) -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_ldts, src_rsrc, src_payload]) -%}
{%- set unique_hash_key = datavault4dbt.expand_column_list(columns=[parent_hash_key, ma_hash_key]) -%}
{%- set source_relation = ref(source_model) -%}

{%- if var('sdcvault.dv_inserted_bool', false) -%}
    {%- set dv_inserted = 'current_timestamp() as ' ~ var('sdcvault.dv_inserted_alias', 'dv_inserted_at') -%}
    {%- set source_cols_inserted = datavault4dbt.expand_column_list(columns=[src_ldts, dv_inserted, src_rsrc, src_payload]) -%}
    {%- set final_columns_to_select = unique_hash_key + [hash_diff_alias, src_ldts, dv_inserted, src_rsrc, 'is_deleted'] + src_payload -%}
{%- else -%}
    {%- set final_columns_to_select = unique_hash_key + [hash_diff_alias, src_ldts, src_rsrc, 'is_deleted'] + src_payload -%}
{%- endif -%}


with

{# Selecting all source data, that is newer than latest data in msat if incremental #}
source_data as (

    select {{ datavault4dbt.print_list(unique_hash_key) }},
        {# Generate Hash Diff based on payload #}
        {{ sdcvault.hash_diff(src_payload, alias=hash_diff_alias, is_case_sensitive=hash_diff_case_sensitive_bool, exclude=hash_diff_exclude) | indent(8) }},
        {{ datavault4dbt.print_list(source_cols) }}
    from {{ source_relation }}

{%- if table_sample_prob != -1 %}
    tablesample ({{ table_sample_prob }})
{% endif -%}

),

{# Get the latest record for each parent_hash_key + ma_hash_key in existing msat, if incremental #}
{%- if is_incremental() %}
latest_entries_in_msat as (

    select *
    from {{ this }}
    qualify row_number() over(partition by {{ parent_hash_key }}, {{ ma_hash_key }} order by {{ src_ldts }} desc) = 1  

),

{# Detect new deleted unique_hash_keys #}
deleted_records as (

    select {{ datavault4dbt.print_list(unique_hash_key) }},
        {{ hash_diff_alias }},
        current_timestamp() as {{ src_ldts }},
        {{ src_rsrc }},
        true as is_deleted,
        {{ datavault4dbt.print_list(src_payload) }}
    from latest_entries_in_msat msat 
    where not exists (
        select 1
        from source_data src
        where {{ datavault4dbt.multikey(unique_hash_key, prefix=['msat','src'], condition='=') | lower }}
    )
        and not coalesce(msat.is_deleted, false)

),

{%- endif %}

{# Union new/changed and deleted records #}
insert_union as (

    select {{ datavault4dbt.print_list(unique_hash_key, src_alias='src') }},
        src.{{ hash_diff_alias }},
{%- if not is_incremental() %}
        src.{{ src_ldts }},
{%- else %}
        {# Use current_timestamp() for reappearing records with old timestamp -#}
        case 
            when ltst.is_deleted and ltst.{{ src_ldts }} >= src.{{ src_ldts }}
                then current_timestamp()
            else src.{{ src_ldts }}
        end as {{ src_ldts }},
{%- endif %}
        src.{{ src_rsrc }},
        false as is_deleted,
        {{ datavault4dbt.print_list(src_payload, src_alias='src') }}
    from source_data src
{%- if is_incremental() %}
    left join latest_entries_in_msat ltst
        on {{ datavault4dbt.multikey(unique_hash_key, prefix=['src', 'ltst'], condition='=') }}
    where
        (
            {{ datavault4dbt.multikey(hash_diff_alias, prefix=['src', 'ltst'], condition='!=') }}
            and src.{{ src_ldts }} > ltst.{{ src_ldts }}
        )
        or ltst.is_deleted

    union all

    select *
    from deleted_records

{%- endif %}

),


records_to_insert as (

    select {{ datavault4dbt.print_list(final_columns_to_select) }}
    from insert_union

)

select * from records_to_insert
{%- endmacro %}