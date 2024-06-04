{%- macro sat(source_model, parent_hash_key, hash_diff_alias, src_payload, src_ldts=none, src_rsrc=none,
              high_water_mark_bool=none, multi_batch_bool=none, hash_diff_exclude=[], hash_diff_case_sensitive_bool=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('sat', 'sdcvault')(source_model=source_model, parent_hash_key=parent_hash_key, hash_diff_alias=hash_diff_alias, src_payload=src_payload,
                                       src_ldts=src_ldts, src_rsrc=src_rsrc, high_water_mark_bool=high_water_mark_bool,
                                       hash_diff_exclude=hash_diff_exclude, hash_diff_case_sensitive_bool=hash_diff_case_sensitive_bool) }}

{% endmacro %}