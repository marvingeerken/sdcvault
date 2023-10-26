{%- set yaml_metadata -%}
source_model: stg_tpch_sf1__customer
derived_columns:
  DV_SOURCE: '!tpch_sf1 customer'
  LAST_UPDATED: current_timestamp()
hashed_columns:
  HK_CUSTOMER: c_custkey
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{{ sdcvault.stg_rv(source_model=metadata_dict['source_model'],
                   derived_columns=metadata_dict['derived_columns'],
                   hashed_columns=metadata_dict['hashed_columns']) }}