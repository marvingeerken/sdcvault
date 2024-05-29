{%- macro default__sat(source_model, parent_hash_key, hash_diff_alias, src_payload,
                       src_ldts, src_rsrc, high_water_mark_bool, table_sample_prob, multi_batch_bool,
                       hash_diff_exclude, hash_diff_case_sensitive_bool) -%}

{%- set src_ldts = sdcvault.replace_standard(src_ldts, 'sdcvault.ldts_alias', 'last_updated') -%}
{%- set src_rsrc = sdcvault.replace_standard(src_rsrc, 'sdcvault.rsrc_alias', 'dv_source') -%}
{%- set high_water_mark_bool = sdcvault.replace_standard(high_water_mark_bool, 'sdcvault.high_water_mark_bool', true) -%}
{%- set table_sample_prob = sdcvault.replace_standard(table_sample_prob, 'sdcvault.table_sample_prob', -1) -%}
{%- set multi_batch_bool = sdcvault.replace_standard(multi_batch_bool, 'sdcvault.multi_batch_bool', false) -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_ldts, src_rsrc, src_payload]) -%}
{%- set final_columns_to_select = [parent_hash_key] + [hash_diff_alias] + source_cols -%}
{%- set source_relation = ref(source_model) -%}


with


{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data as (

    select
        {{ parent_hash_key }},
        {# Generate Hash Diff based on payload #}
        {{ sdcvault.hash_diff(src_payload, alias=hash_diff_alias, is_case_sensitive=hash_diff_case_sensitive_bool, exclude=hash_diff_exclude) | indent(8) }},
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

{# Get the latest record for each parent hashkey in existing sat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_sat as (

    select
        {{ parent_hash_key }},
        {{ hash_diff_alias }},
        {{ src_ldts }}
    from {{ this }}
    qualify row_number() over(partition by {{ parent_hash_key }} order by {{ src_ldts }} desc) = 1  

),
{%- endif %}

{%- if multi_batch_bool %}
{#
    Deduplicate source by comparing each hash diff to the hash diff of the previous record, for each hash key.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_source_data as (

    select {{ datavault4dbt.print_list(final_columns_to_select) }}
    {%- if is_incremental() %},
    row_number() over(partition by {{ parent_hash_key }} order by {{ src_ldts }}) as rn
    {%- endif %}
    from source_data
    qualify
        case
            when {{ hash_diff_alias }} = lag({{ hash_diff_alias }}) over(partition by {{ parent_hash_key }} order by {{ src_ldts }}) then false
            else true
        end

),
{%- endif %}

{#
    select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}
records_to_insert as (

    select {{ datavault4dbt.print_list(final_columns_to_select) }}
    from {% if multi_batch_bool -%} deduplicated_ {%- endif -%} source_data src
{%- if is_incremental() %}
    where not exists (
        select 1
        from latest_entries_in_sat ltst
        where
            {{ datavault4dbt.multikey(parent_hash_key, prefix=['ltst', 'src'], condition='=') }}
            and (
                {{ datavault4dbt.multikey(hash_diff_alias, prefix=['ltst', 'src'], condition='=') }}
                or ltst.{{ src_ldts }} >= src.{{ src_ldts }}
            )
    {%- if multi_batch_bool -%}
            and src.rn = 1
    {%- endif %}
    )
{%- endif %}

)

select * from records_to_insert
{%- endmacro %}