{#
    Macro to create Satellites.
    This macro only works on source data, that has a unique Primary Key.
    On a source having multiple records per key (e.g. PSA) is will create wrong results.
#}

{%- macro default__sat(src_pk, src_payload, src_ldts, src_source, source_model, hashdiff, hashdiff_is_case_sensitive, hashdiff_exclude) -%}

{%- set all_cols = automate_dv.expand_column_list(columns=[src_pk, hashdiff, src_ldts, src_source, src_payload]) -%}
{%- set all_cols_no_hd = automate_dv.expand_column_list(columns=[src_pk, src_payload, src_ldts, src_source]) -%}
{%- set cols_latest_records = automate_dv.expand_column_list(columns=[src_pk, hashdiff, src_ldts]) -%}


with source_data as (
    select
    {{ automate_dv.prefix(all_cols_no_hd, 'stg') }},
    {{ sdcvault.hashdiff(src_payload, alias=hashdiff, is_case_sensitive=hashdiff_is_case_sensitive, exclude=hashdiff_exclude) }}
    from {{ ref(source_model) }} as stg

    {%- if var('sdcvault.high_water_mark') and is_incremental() %}
    where {{ automate_dv.prefix([src_ldts], 'stg') }} > (
            select max({{ automate_dv.prefix([src_ldts], 'sat') }})
            from {{ this }} as sat
    )
    {%- endif %}
),

{%- if is_incremental() %}

latest_records as (
    select {{ automate_dv.prefix(cols_latest_records, 'sat') }}
    from {{ this }} as sat
    inner join (
        select distinct {{ automate_dv.prefix([src_pk], 'stg') }}
        from source_data stg
    ) as stg
        on {{ automate_dv.multikey(src_pk, prefix=['sat','stg'], condition='=') }}
    qualify row_number() over (partition by {{ automate_dv.prefix([src_pk], 'sat') }} order by {{ automate_dv.prefix([src_ldts], 'sat') }} desc) = 1
),

{%- endif %}

records_to_insert as (
    select distinct {{ automate_dv.prefix(all_cols, 'stg') }}
    from source_data as stg
    {%- if is_incremental() %}
        left join latest_records sat
            on {{ automate_dv.multikey(src_pk, prefix=['sat','stg'], condition='=') }}
            where ({{ automate_dv.prefix([hashdiff], 'sat') }} != {{ automate_dv.prefix([hashdiff], 'stg') }}
                and {{ automate_dv.prefix([src_ldts], 'stg') }} > {{ automate_dv.prefix([src_ldts], 'sat') }})
                or {{ automate_dv.prefix([hashdiff], 'sat') }} is null
    {%- endif %}
)

select * from records_to_insert

{%- endmacro -%}