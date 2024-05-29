{%- macro bmsat_curr(bv_curr_parent, rv_satellite, hash_key, ma_hash_key) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('bmsat_curr', 'sdcvault')(bv_curr_parent=bv_curr_parent, rv_satellite=rv_satellite, hash_key=hash_key, ma_hash_key=ma_hash_key) }}

{% endmacro %}