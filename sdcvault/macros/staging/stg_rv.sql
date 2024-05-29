{# creation of stg_rv models #}

{%- macro stg_rv(include_source_columns=none, source_model=none, hashed_columns=none, derived_columns=none) -%}

    {{ sdcvault.prepend_generated_by() }}

    {{ adapter.dispatch('stg_rv', 'sdcvault')(include_source_columns=include_source_columns, source_model=source_model, 
                                              hashed_columns=hashed_columns, derived_columns=derived_columns) -}}

{%- endmacro -%}


{%- macro default__stg_rv(include_source_columns, source_model, hashed_columns, derived_columns) -%}

    {%- if include_source_columns is none -%}
        {%- set include_source_columns = true -%}
    {%- endif -%}


{#- check for source format or ref format and create relation object from source_model -#}
{% if source_model is mapping and source_model is not none -%}

    {%- set source_name = source_model | first -%}
    {%- set source_table_name = source_model[source_name] -%}

    {%- set source_relation = source(source_name, source_table_name) -%}
    {%- set all_source_columns = automate_dv.source_columns(source_relation=source_relation) -%}
{%- elif source_model is not mapping and source_model is not none -%}

    {%- set source_relation = ref(source_model) -%}
    {%- set all_source_columns = automate_dv.source_columns(source_relation=source_relation) -%}
{%- else -%}

    {%- set all_source_columns = [] -%}
{%- endif -%}

{%- set derived_column_names = automate_dv.extract_column_names(derived_columns) | map('upper') | list -%}
{%- set hashed_column_names = automate_dv.extract_column_names(hashed_columns) | map('upper') | list -%}
{%- set exclude_column_names = (derived_column_names + hashed_column_names)  | map('upper') | list -%}
{%- set source_and_derived_column_names = (all_source_columns + derived_column_names) | map('upper') | unique | list -%}

{%- set source_columns_to_select = automate_dv.process_columns_to_select(all_source_columns, exclude_column_names) -%}
{%- set derived_columns_to_select = automate_dv.process_columns_to_select(source_and_derived_column_names, hashed_column_names) | unique | list -%}
{%- set final_columns_to_select = [] -%}

{#- include source columns in final column selection if true -#}
{%- if include_source_columns -%}
    {%- if automate_dv.is_nothing(derived_columns)
           and automate_dv.is_nothing(hashed_columns) -%}
        {%- set final_columns_to_select = final_columns_to_select + all_source_columns -%}
    {%- else -%}
        {#- only include non-overriden columns if not just source columns -#}
        {%- set final_columns_to_select = final_columns_to_select + source_columns_to_select -%}
    {%- endif -%}
{%- endif %}

with source_data as (

    select

    {{- "\n\n    " ~ automate_dv.print_list(automate_dv.escape_column_names(all_source_columns)) if all_source_columns else " *" }}

    from {{ source_relation }}
    {%- set last_cte = "source_data" %}
)

{%- if automate_dv.is_something(derived_columns) -%},

derived_columns as (

    select

    {{ automate_dv.derive_columns(source_relation=source_relation, columns=derived_columns) | indent(4) }}

    from {{ last_cte }}
    {%- set last_cte = "derived_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + derived_column_names %}
)
{%- endif -%}
--{{automate_dv.is_something(hashed_columns)}}

{% if automate_dv.is_something(hashed_columns) -%},
--{{automate_dv.is_something(hashed_columns)}}
hashed_columns as (

    select

    {{ automate_dv.print_list(automate_dv.escape_column_names(derived_columns_to_select)) }},

    {% set processed_hash_columns = automate_dv.process_hash_column_excludes(hashed_columns, all_source_columns) -%}
    {{- sdcvault.hash_columns(columns=processed_hash_columns) | indent(4) }}

    from {{ last_cte }}
    {%- set last_cte = "hashed_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + hashed_column_names %}
)
{%- endif -%}
,

columns_to_select as (

    select

    {{ automate_dv.print_list(automate_dv.escape_column_names(final_columns_to_select | unique | list)) }}

    from {{ last_cte }}
),

default_values as (
    select
    {% for col in final_columns_to_select | unique | list -%}
        {{ sdcvault.ghost_record(col)}}{%- if not loop.last -%},{% endif %}
    {% endfor -%}
)

select * from columns_to_select
union all 
select * from default_values
{%- endmacro -%}

select * from columns_to_select
{%- endmacro -%}