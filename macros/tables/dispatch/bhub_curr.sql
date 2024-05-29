{#- 
    This macro creates Business Hubs, that show the current version. 
    A Business Hub joins the Effectivity Satellite to remove deleted Business Keys. 
-#}

{%- macro bhub_curr(rv_hub, rv_esat, hash_key) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('bhub_curr', 'sdcvault')(rv_hub=rv_hub, rv_esat=rv_esat, hash_key=hash_key) }}

{% endmacro %}