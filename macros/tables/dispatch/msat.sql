{#
    Macro to create Muli-Active Satellites.
    In most cases we have particular columns, which define the multi-activity in our MSATs.
    This is the case, because they define the Primary Key in the source.
    We can add those to our MSAT PK to easily load them into our Raw Vault.

    The only thing that is missing is the delete detection on those PK combination, since our ESATs work on our Hub HK without MA-Key.
    To solve this the delete detection of HK+MA-Key is done directly inside the MSAT doing the load.
    A new metadata field "is_deleted" is introduced.

    This macro only works on source data, that has a unique Primary Key.
    On a source having multiple records per PK (e.g. PSA) it will create wrong results.
#}

{%- macro msat(source_model, parent_hash_key, ma_hash_key, hash_diff_alias, src_payload,
               src_ldts=none, src_rsrc=none, table_sample_prob=none, multi_batch_bool=none,
               hash_diff_exclude=[], hash_diff_case_sensitive_bool=none) -%}

{{- sdcvault.prepend_generated_by() }}

{{ adapter.dispatch('msat', 'sdcvault')(source_model=source_model, parent_hash_key=parent_hash_key, ma_hash_key=ma_hash_key, hash_diff_alias=hash_diff_alias, 
                                        src_payload=src_payload, src_ldts=src_ldts, src_rsrc=src_rsrc,
                                        table_sample_prob=table_sample_prob, multi_batch_bool=multi_batch_bool,
                                        hash_diff_exclude=hash_diff_exclude, hash_diff_case_sensitive_bool=hash_diff_case_sensitive_bool) }}

{% endmacro %}