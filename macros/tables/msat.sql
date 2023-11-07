{#
    Macro to create Muli-Active Satellites.
    In most cases we have particular columns, which define the multi-activity in our MSATs.
    This is the case, because they define the Primary Key in the source.
    We can add those to our MSAT PK to easily load them into our Raw Vault.

    The only thing that is missing is the delete detection on those PK combination, since our ESATs work on our Hub HK without MA-Key.
    To solve this the delete detection of HK+MA-Key is done directly inside the MSAT doing the load.
    A new metadata field "is_deleted" is introduced.

    This macro only works on source data, that has a unique Primary Key.
    On a source having multiple records per PK (e.g. PSA) it will create wrong results.
#}


{%- macro default__msat(src_pk, src_payload, src_ldts, src_source, source_model, hashdiff, hashdiff_is_case_sensitive, hashdiff_exclude) -%}

{%- set all_cols = automate_dv.expand_column_list(columns=[src_pk, hashdiff, src_ldts, src_source, src_payload]) -%}
{%- set all_cols_no_hd = automate_dv.expand_column_list(columns=[src_pk, src_payload, src_ldts, src_source]) -%}
{%- set del_cols_1 = automate_dv.expand_column_list(columns=[src_pk, hashdiff]) -%}
{%- set del_cols_2 = automate_dv.expand_column_list(columns=[src_source, src_payload]) -%}

with source_data as (
    select
    {{ automate_dv.prefix(all_cols_no_hd, 'stg') }},
    {{ sdcvault.hashdiff(src_payload, alias=hashdiff, is_case_sensitive=hashdiff_is_case_sensitive, exclude=hashdiff_exclude) }}
    from {{ ref(source_model) }} as stg
),

{%- if is_incremental() %}

latest_records as (
    select
        {{ automate_dv.prefix(all_cols, 'sat') }},
        sat.is_deleted
    from {{ this }} as sat
    where {{ automate_dv.prefix([src_source], 'sat') }} != {{ var('sdcvault.ghost_source') }}
    qualify row_number() over (partition by {{ automate_dv.prefix([src_pk], 'sat') }} order by {{ automate_dv.prefix([src_ldts], 'sat') }} desc) = 1
),

deleted_records as (
    select
        {{ automate_dv.prefix(del_cols_1, 'sat') }},
        to_timestamp_ntz(current_timestamp()) as {{ src_ldts }},
        {{ automate_dv.prefix(del_cols_2, 'sat') }}
    from latest_records sat 
    where not exists (
        select 1
        from source_data stg
        where {{ automate_dv.multikey(src_pk, prefix=['sat','stg'], condition='=') }}
    )
        and not coalesce(sat.is_deleted, false)
),

{%- endif %}

records_to_insert as (
    select distinct 
        {{ automate_dv.alias_all(stg, 'stg') }},
        false as is_deleted
    from source_data as stg
    {%- if is_incremental() %}
    left join latest_records sat
        on {{ automate_dv.multikey(src_pk, prefix=['sat','stg'], condition='=') }}
        where (
                {{ automate_dv.prefix([hashdiff], 'sat') }} != {{ automate_dv.prefix([hashdiff], 'stg') }}
                or sat.is_deleted
            )
            and {{ automate_dv.prefix([src_ldts], 'stg') }} > {{ automate_dv.prefix([src_ldts], 'sat') }}
            or {{ automate_dv.prefix([src_hashdiff], 'sat') }} is null

    union all

    select
        {{ automate_dv.prefix(all_cols, 'deleted') }},
        true as is_deleted
    from deleted_records as deleted
    {%- endif %}
)

select * from records_to_insert

{%- endmacro -%}
