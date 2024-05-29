{%- set yaml_metadata -%}
hash_key: hk_customer
hash_key_ma: 
bhub: customer_curr_bh
sat: customer_tpch_sf1_s
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sdcvault.bsat(hash_key=metadata_dict["hash_key"],
                 hash_key_ma=metadata_dict["hash_key_ma"],
                 bhub=metadata_dict["bhub"],
                 sat=metadata_dict["sat"]) }} 