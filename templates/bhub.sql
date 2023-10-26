{{ config(materialized='ephemeral')}}

{%- set yaml_metadata -%}
hub: customer_h
esat: customer_es
hash_key: hk_customer
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sdcvault.bhub(hub=metadata_dict["hub"], 
                 esat=metadata_dict["esat"], 
                 hash_key=metadata_dict["hash_key"]) }} 