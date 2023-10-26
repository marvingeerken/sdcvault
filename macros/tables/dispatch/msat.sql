{%- macro msat(src_pk, src_payload, src_ldts, src_source, source_model, hashdiff, hashdiff_is_case_sensitive, hashdiff_exclude=[]) -%}

    {{ sdcvault.prepend_generated_by() }}

    {{ adapter.dispatch('msat', 'sdcvault')(src_pk=src_pk, src_payload=src_payload, src_ldts=src_ldts, 
                                    src_source=src_source, source_model=source_model, hashdiff=hashdiff, 
                                    hashdiff_is_case_sensitive=hashdiff_is_case_sensitive, hashdiff_exclude=hashdiff_exclude) -}}

{%- endmacro -%}