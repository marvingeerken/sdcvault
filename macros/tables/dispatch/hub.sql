{%- macro hub(source_models, hash_key, business_key, src_ldts=none, src_rsrc=none, high_water_mark_bool=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('hub', 'sdcvault')(source_models=source_models, hash_key=hash_key, business_key=business_key, src_ldts=src_ldts, src_rsrc=src_rsrc,
                                       high_water_mark_bool=high_water_mark_bool) }}

{% endmacro %}