{%- macro natural_key(field_list) -%}

    {{ adapter.dispatch('natural_key', 'sdcvault')(field_list=field_list) -}}

{%- endmacro -%}


{% macro default__natural_key(field_list) -%}

{%- macro natural_key(field_list, varchar_length=120) -%}

{%- set all_null = [] -%}
{%- for field in field_list -%}
    {%- if not loop.last -%}
        {%- do all_null.append('-') -%}
    {%- endif -%}
{%- endfor -%}

{%- set fields = [] -%}

{%- for field in field_list -%}

    {%- do fields.append(
        "coalesce(" ~ field ~ "::varchar, '')"
    ) -%} {%- endfor %}
        coalesce(
            nullif(
                {% for field in fields -%}
                {{ field }} {%- if not loop.last %} || '-' || {%- else -%}, {%- endif %}
                {% endfor -%}
                '{{ all_null | join("") }}'
            ),
            {{ var('sdcvault.ghost_hk') }}
        )::varchar({{ varchar_length }})

{%- endmacro %}