{# Adaptation of the dbtvault hashing by replacing it with our surrogate_key implementation. #}

{%- macro hash_columns(columns=none, columns_to_escape=none) -%}

    {{ adapter.dispatch('hash_columns', 'sdcvault')(columns=columns, columns_to_escape=columns_to_escape) -}}

{%- endmacro -%}


{%- macro default__hash_columns(columns, columns_to_escape) -%}

{%- if columns is mapping and columns is not none -%}

    {%- for col in columns -%}

        {%- if columns[col] is string -%}

            {{ sdcvault.surrogate_key(columns[col].split(',')) }} as {{col}}

        {%- else -%}

            {{ sdcvault.surrogate_key(columns[col]) }} as {{col}}

        {%- endif -%}

        {{- ",\n" if not loop.last -}}
    {%- endfor -%}

{%- endif %}
{%- endmacro -%}