{#-
    This macro creates Effectivity Satellites attached to Hubs or Links using the Primary Hash Key.

    To allow parallel Raw Vault loading it uses the Esat own PKs to be compared to the stage.
    That means the Hub/Link can hold PKs, that are not in the Esat. For example when the Hub/Links has been run own its own.
    That scenario should be considered in the Business Vault.

    Its also not possible to calculate the actual delete timestamp, because we dont get this information as for instance from CDC.
    For that pupose the current_timestamp() of the Esat execution is taken.


    doesnt work with multi source on multi batch (no macro will)

-#}

{%- macro esat(source_models, parent_hash_key, src_ldts=none, src_rsrc=none, high_water_mark_bool=none, table_sample_prob=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('esat', 'sdcvault')(source_models=source_models, parent_hash_key=parent_hash_key,
                                        src_ldts=src_ldts, src_rsrc=src_rsrc, high_water_mark_bool=high_water_mark_bool, table_sample_prob=table_sample_prob) }}

{% endmacro %}