{%- macro blink(hash_key_link, esat, link, bhubs) -%}

    {{ sdcvault.prepend_generated_by() }}

    {{ adapter.dispatch('blink', 'sdcvault')(hash_key_link=hash_key_link, esat=esat, link=link, bhubs=bhubs) -}}

{%- endmacro -%}