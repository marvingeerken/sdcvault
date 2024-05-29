{%- macro surrogate_key(field_list) -%}

    {{ adapter.dispatch('surrogate_key', 'sdcvault')(field_list=field_list) -}}

{%- endmacro -%}


{%- macro default__surrogate_key(field_list) -%}

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
            md5_binary(
                nullif(
                    {% for field in fields -%}
                    {{ field }} {%- if not loop.last %} || '-' || {%- else -%}, {%- endif %}
                    {% endfor -%}
                    '{{ all_null | join("") }}'
                )
            ),
            {{ var('sdcvault.ghost_hk') }}::binary(16)
        )

{%- endmacro %}