{%- macro bsat(hash_key, hash_key_ma, bhub, sat, ldts='last_updated') -%}

    {{ sdcvault.prepend_generated_by() }}

    {{ adapter.dispatch('bsat', 'sdcvault')(hash_key=hash_key, hash_key_ma=hash_key_ma, bhub=bhub, sat=sat, ldts=ldts) -}}

{%- endmacro -%}