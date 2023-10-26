{%- macro surrogate_key(field_list) -%}

    {{ adapter.dispatch('surrogate_key', 'sdcvault')(field_list=field_list) -}}

{%- endmacro -%}


{%- macro default__surrogate_key(field_list) -%}

    {%- set all_null = [] -%}
    {%- for field in field_list -%}
        {%- if not loop.last %}{% do all_null.append('-') -%}{%- endif -%}
    {%- endfor -%}

    coalesce({{- dbt_utils.generate_surrogate_key( field_list ) | replace('md5_binary(', 'md5_binary(nullif(') | replace('as \n    varchar\n)', 'as varchar )') | replace('))', "), '" ~ (all_null | join("")) ~ "') ") }} ), '00000000000000000000000000000000'::binary(16))

{%- endmacro -%}