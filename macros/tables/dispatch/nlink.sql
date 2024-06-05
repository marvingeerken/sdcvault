{%- macro nlink(source_models, link_hash_key, foreign_hash_keys, src_payload, src_ldts=none, src_rsrc=none,
               high_water_mark=none, multi_batch_bool=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('nlink', 'sdcvault')(source_models=source_models, link_hash_key=link_hash_key, foreign_hash_keys=foreign_hash_keys, src_payload=src_payload,
                                         src_ldts=src_ldts, src_rsrc=src_rsrc, high_water_mark=high_water_mark, multi_batch_bool=multi_batch_bool) }}

{% endmacro %}