{{ config(materialized='incremental',
          on_schema_change='append_new_columns') }}

{%- set yaml_metadata -%}
source_model: stg_rv_customer_tpch_sf1
src_pk: HK_CUSTOMER
hashdiff: HD_CUSTOMER_TPCH_SF1_S
src_payload:
  - C_NAME
src_ldts: LAST_UPDATED
src_source: DV_SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sdcvault.sat(src_pk=metadata_dict["src_pk"],
                hashdiff=metadata_dict["hashdiff"],
                src_payload=metadata_dict["src_payload"],
                src_ldts=metadata_dict["src_ldts"],
                src_source=metadata_dict["src_source"],
                source_model=metadata_dict["source_model"]) }}