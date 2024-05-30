{%- macro nlink(source_models, link_hash_key, foreign_hash_keys, src_payload, src_ldts=none, src_rsrc=none,
               high_water_mark_bool=none, limit_sources_num=none, table_sample_prob=none, multi_batch_bool=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('nlink', 'sdcvault')(source_models=source_models, link_hash_key=link_hash_key, foreign_hash_keys=foreign_hash_keys, src_payload=src_payload, src_ldts=src_ldts,
                                         src_rsrc=src_rsrc, high_water_mark_bool=high_water_mark_bool, limit_sources_num=limit_sources_num, table_sample_prob=table_sample_prob,
                                         multi_batch_bool=multi_batch_bool) }}

{% endmacro %}