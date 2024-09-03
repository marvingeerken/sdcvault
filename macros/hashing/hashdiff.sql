{%- macro hash_diff(columns=none, alias=none, is_case_sensitive=false, exclude=[]) -%}

    {{ adapter.dispatch('hash_diff', 'sdcvault')(columns=columns, alias=alias, is_case_sensitive=is_case_sensitive, exclude=exclude) -}}

{%- endmacro -%}


{%- macro default__hash_diff(columns, alias, is_case_sensitive, exclude) -%}

{%- set hash_alg = 'md5_binary' -%}
{%- set hash_size = 16 -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- if is_case_sensitive -%}
    {%- set standardise = "nullif(trim(cast([expression] as varchar)), '')" %}
{%- else -%}
    {%- set standardise = "nullif(upper(trim(cast([expression] as varchar))), '')" %}
{%- endif -%}

{#- alpha sort columns before hashing  -#}
{%- set columns = columns|sort -%}

{#- if single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = sdcvault.as_constant(columns) -%}
    {%- set escaped_column_str = column_str -%}

    {{- "cast(({}({})) as binary({})) as {}".format(hash_alg, standardise | replace('[expression]', escaped_column_str), hash_size, alias) | indent(4) -}}

{#- else a list of columns to hash -#}
{%- else -%}
    {%- set all_null = [] -%}

    {{- "cast({}(concat_ws('{}',".format(hash_alg, concat_string) | indent(4) -}}

    {%- for column in columns -%}

        {% if not column in exclude %}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- set column_str = sdcvault.as_constant(column) -%}
        {%- set escaped_column_str = column_str -%}

        {{- "\nifnull({}, '{}')".format(standardise | replace('[expression]', escaped_column_str), null_placeholder_string) | indent(4) -}}
        {{- "," if not loop.last -}}

        {%- if loop.last -%}

            {{- "\n)) as binary({})) as {}".format(hash_size, alias) -}}

        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

        {% endif %}

    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}