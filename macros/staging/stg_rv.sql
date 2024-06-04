{# creation of stg_rv models #}

{%- macro stg_rv(source_model=none, hashed_columns=none, derived_columns=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('stg_rv', 'sdcvault')(source_model=source_model, hashed_columns=hashed_columns, derived_columns=derived_columns) }}

{% endmacro %}


{%- macro default__stg_rv(source_model, hashed_columns, derived_columns) -%}

{#- Check for source format or ref format and create relation object from source_model -#}
{%- if source_model is mapping and source_model is not none -%}

    {%- set source_name = source_model | first -%}
    {%- set source_table_name = source_model[source_name] -%}

    {%- set source_relation = source(source_name, source_table_name) -%}
    {%- set all_source_columns = datavault4dbt.source_columns(source_relation=source_relation) | map('lower') | list -%}

{%- elif source_model is not mapping and source_model is not none -%}

    {{- log('source_model is not mapping and not none: ' ~ source_model, false) -}}

    {%- set source_relation = ref(source_model) -%}
    {%- set all_source_columns = datavault4dbt.source_columns(source_relation=source_relation) | map('lower') | list -%}
{%- else -%}
    {%- set all_source_columns = [] -%}
{%- endif -%}

{%- set derived_column_names = datavault4dbt.extract_column_names(derived_columns) -%}
{%- set hashed_column_names = datavault4dbt.extract_column_names(hashed_columns) -%}
{%- set exclude_column_names = (derived_column_names + hashed_column_names) -%}
{%- set source_and_derived_column_names = (all_source_columns + derived_column_names) | unique | list -%}

{%- set source_columns_to_select = datavault4dbt.process_columns_to_select(all_source_columns, exclude_column_names) -%}
{%- set derived_columns_to_select = datavault4dbt.process_columns_to_select(source_and_derived_column_names, hashed_column_names) | unique | list -%}
{%- set final_columns_to_select = source_columns_to_select -%}


with

source_data as (

    select {{ datavault4dbt.print_list(all_source_columns) if all_source_columns else " *" }}
    from {{ source_relation }}
    {% set last_cte = "source_data" %}
),

{% if datavault4dbt.is_something(derived_columns) %}
derived_columns as (

    select
        {{ sdcvault.derive_columns(source_relation=source_relation, columns=derived_columns) | indent(8) }}

    from {{ last_cte }}
    {%- set last_cte = "derived_columns" %}
    {% set final_columns_to_select = final_columns_to_select + derived_column_names %}
),
{%- endif %}

{% if datavault4dbt.is_something(hashed_columns) -%}
hashed_columns as (

    select {{ datavault4dbt.print_list(derived_columns_to_select) }},

    {%- set processed_hash_columns = datavault4dbt.process_hash_column_excludes(hashed_columns, all_source_columns) %}
    {{ sdcvault.hash_columns(columns=processed_hash_columns) }}

    from {{ last_cte }}
    {%- set last_cte = "hashed_columns" %}
    {% set final_columns_to_select = final_columns_to_select + hashed_column_names %}
),
{%- endif %}


columns_to_select as (

    select {{ datavault4dbt.print_list(final_columns_to_select | unique | list) }}
    from {{ last_cte }}

),


default_values as (

    select
{%- for col in final_columns_to_select | unique | list %}
        {{ sdcvault.ghost_record(col) }} {%- if not loop.last -%},{%- endif %}
{%- endfor %}

),


final as (

    select * from columns_to_select
    union all 
    select * from default_values

)


select * from final
{%- endmacro %}