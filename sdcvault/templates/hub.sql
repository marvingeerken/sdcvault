{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: stg_rv_customer_tpch_sf1
src_pk: HK_CUSTOMER
src_nk: C_CUSTKEY
src_ldts: LAST_UPDATED
src_source: DV_SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.hub(src_pk=metadata_dict["src_pk"],
                   src_nk=metadata_dict["src_nk"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"]) }}