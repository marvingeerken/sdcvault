{#-
    This macro creates Effectivity Satellites attached to Hubs or Links using the Primary Hash Key.

    To allow parallel Raw Vault loading it uses the Esat own PKs to be compared to the stage.
    That means the Hub/Link can hold PKs, that are not in the Esat. For example when the Hub/Links has been run own its own.
    That scenario should be considered in the Business Vault.

    Its also not possible to calculate the actual delete timestamp, because we dont get this information as for instance from CDC.
    For that pupose the current_timestamp() of the Esat execution is taken.
-#}


{%- macro default__esat(src_pk, src_ldts, src_source, source_model) -%}

{%- set source_cols = automate_dv.expand_column_list(columns=[src_pk, src_ldts, src_source]) -%}

with

{% if not (source_model is iterable and source_model is not string) -%}
    {%- set source_model = [source_model] -%}
{%- endif -%}


{#- Get available HKs from stage -#}
src_union as (
    {% for src in source_model -%}
    select {{ automate_dv.prefix(source_cols, 'stg') }}
    from {{ ref(src) }} stg
    {% if not loop.last %}union all{% endif %}
    {% endfor -%}
),


{# Distinct HKs -#}
src_union_first as (
    select {{ automate_dv.prefix(source_cols, 'stg') }}
    from src_union stg
    qualify row_number() over (partition by {{ automate_dv.prefix([src_pk], 'stg') }} 
        order by {{ automate_dv.prefix([src_ldts], 'stg') }}, {{ automate_dv.prefix([src_source], 'stg') }}) = 1
),


{% if is_incremental() -%}
{# Get latest record per key from esat in incremental runs -#}
esat_latest as (
    select *
    from {{ this }} sat
    qualify row_number() over (partition by {{ automate_dv.prefix([src_pk], 'sat') }} order by {{ automate_dv.prefix([src_ldts], 'sat') }} desc) = 1
),
{%- endif %}


{# Prepare insert -#}
insert_rows as (

    {# Insert records from hub with is_deleted=false, if its not yet available -#}
    select
        {{ automate_dv.prefix(source_cols, 'stg') }},
        {{ src_ldts }} as start_date,
        to_timestamp_ntz('9999-12-31') AS end_date,
        false as is_deleted 
    from src_union_first stg

{#- Following input matters on incremental runs only -#}
{%- if is_incremental() %}
    where {{ automate_dv.prefix([src_pk], 'stg') }} not in (
        select {{ automate_dv.prefix([src_pk], 'sat') }}
        from esat_latest sat
    )

    union all

    {# Insert records from hub with is_deleted=true, if they are not available in stage and not yet as deleted esat -#}
    select
        {{ automate_dv.prefix([src_pk], 'sat') }},
        to_timestamp_ntz(current_timestamp()) as {{ src_ldts }},
        {{ automate_dv.prefix([src_source], 'sat') }},
        {{ automate_dv.prefix([src_ldts], 'sat') }} as start_date,
        to_timestamp_ntz(current_timestamp()) AS end_date,
        true as is_deleted
    from esat_latest sat
    where not is_deleted 
        and {{ automate_dv.prefix([src_pk], 'sat') }} not in (
            select {{ automate_dv.prefix([src_pk], 'stg') }}
            from src_union_first stg
        )

    union all

    {# Insert records from stage with is_deleted=false, if latest esat entry is is_deleted=true => HK is available again -#}
    select
        {{ automate_dv.prefix([src_pk], 'stg') }},

        {# Use current_timestamp() as ldts for recurring keys with old ldts. Otherwise we would get Unique PK violation. -#}
        iff({{ automate_dv.prefix([src_ldts], 'stg') }} > {{ automate_dv.prefix([src_ldts], 'sat') }}, 
            {{ automate_dv.prefix([src_ldts], 'stg') }},
            to_timestamp_ntz(current_timestamp())
           ) as {{ src_ldts }},

        {{ automate_dv.prefix([src_source], 'stg') }},
        {{ automate_dv.prefix([src_ldts], 'stg') }} as start_date,
        to_timestamp_ntz('9999-12-31') as end_date,
        false as is_deleted 
    from src_union_first stg
    left join esat_latest sat
        on {{ automate_dv.multikey(src_pk, prefix=['stg','sat'], condition='=') }}
    where {{ automate_dv.prefix([src_pk], 'sat') }} is not null
        and sat.is_deleted

{%- endif %}
)

select *
from insert_rows

{%- endmacro -%}