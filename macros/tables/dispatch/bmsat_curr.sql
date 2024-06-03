{#- 
    This macro creates Business Multi-Active Satellites, that show the current version. 
    It uses the integrated is_deleted field of the MSAT to remove deleted keys (parent + ma-key).
-#}

{%- macro bmsat_curr(bv_curr_parent, rv_ma_satellite, hash_key, ma_hash_key) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('bmsat_curr', 'sdcvault')(bv_curr_parent=bv_curr_parent, rv_ma_satellite=rv_ma_satellite, hash_key=hash_key, ma_hash_key=ma_hash_key) }}

{% endmacro %}