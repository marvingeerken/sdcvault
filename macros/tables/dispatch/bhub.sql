{%- macro bhub(hash_key, hub, esat) -%}

    {{ sdcvault.prepend_generated_by() }}

    {{ adapter.dispatch('bhub', 'sdcvault')(hash_key=hash_key, hub=hub, esat=esat) -}}

{%- endmacro -%}