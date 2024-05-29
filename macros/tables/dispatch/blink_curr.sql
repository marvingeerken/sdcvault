{#- 
    This macro creates Business Links. 
    A Business Link joins the Effectivity Satellite to remove deleted relationships.
    It also joins the Business Hub to remove relationships, that hold deleted Business Keys.
-#}

{%- macro blink_curr(rv_link, rv_esat, bv_curr_hubs, link_hash_key) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('blink_curr', 'sdcvault')(rv_link=rv_link, rv_esat=rv_esat, bv_curr_hubs=bv_curr_hubs, link_hash_key=link_hash_key) }}

{% endmacro %}