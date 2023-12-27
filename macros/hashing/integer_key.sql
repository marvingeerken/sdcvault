{%- macro integer_key(field_list) -%}

    {{ adapter.dispatch('integer_key', 'sdcvault')(field_list=field_list) -}}

{%- endmacro -%}


{% macro default__integer_key(field_list) -%}

{%- set all_null = [] -%}
{%- for field in field_list -%}
    {%- if not loop.last %}{% do all_null.append('-') -%}{%- endif -%}
{%- endfor -%}

{%- set fields = [] -%}

{%- for field in field_list -%}

    {%- do fields.append(
        "coalesce(cast(" ~ field ~ " as varchar), '')"
    ) -%}

    {%- if not loop.last %}
        {%- do fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}

md5_number_lower64(coalesce(nullif({{ dbt.concat(fields) }}, '{{ all_null | join("") }}'), '00000000000000000000000000000000'))

{%- endmacro %}