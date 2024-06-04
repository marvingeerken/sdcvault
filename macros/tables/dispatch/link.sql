{%- macro link(source_models, link_hash_key, foreign_hash_keys, src_ldts=none, src_rsrc=none, high_water_mark_bool=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('link', 'sdcvault')(source_models=source_models, link_hash_key=link_hash_key, foreign_hash_keys=foreign_hash_keys, src_ldts=src_ldts, src_rsrc=src_rsrc,
                                        high_water_mark_bool=high_water_mark_bool) }}

{% endmacro %}