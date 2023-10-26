{%- macro esat(src_pk, src_ldts, src_source, source_model) -%}

    {{ sdcvault.prepend_generated_by() }}

    {{ adapter.dispatch('esat', 'sdcvault')(src_pk=src_pk, src_ldts=src_ldts, src_source=src_source, source_model=source_model) -}}

{%- endmacro -%}