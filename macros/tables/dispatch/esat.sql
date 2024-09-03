{#-
    This macro creates Effectivity Satellites attached to Hubs or Links using the Primary Hash Key.

    To allow parallel Raw Vault loading it uses the ESAT own PKs to be compared to the stage.
    That means the Hub/Link can hold PKs, that are not in the ESAT. For example when the Hub/Links has been run own its own.
    That scenario should be considered in the Business Vault.

    Its also not possible to calculate the actual is_delete timestamp, because we dont get this information as for instance from CDC.
    For that pupose the current_timestamp() of the ESAT execution is taken.

    This macro doesnt work on multi batch stages / PSAs.
    Also when combining multiple sources is only detects deletes over all soources. Create an ESAT per source if the information is required on this granularity.

-#}

{%- macro esat(source_models, parent_hash_key, src_ldts=none, src_rsrc=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('esat', 'sdcvault')(source_models=source_models, parent_hash_key=parent_hash_key, src_ldts=src_ldts, src_rsrc=src_rsrc) }}

{% endmacro %}