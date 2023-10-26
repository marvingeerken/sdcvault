{%- macro ghost_record(column) -%}

    {{ adapter.dispatch('ghost_record', 'sdcvault')(column=column) -}}

{%- endmacro -%}


{% macro default__ghost_record(column) -%}

{%- set re = modules.re -%}

{%- if re.match('hk_.*|hd_.*', column.lower()) -%}
      {{var('sdcvault.ghost_hk')}}::binary(16) as {{ column.lower() }}
{%- elif column.lower() == 'last_updated' -%}
      {{var('sdcvault.ghost_ts')}}::timestamp_ntz as {{ column.lower() }}
{%- elif column.lower() == 'dv_source' -%}
      {{var('sdcvault.ghost_source')}}::varchar as {{ column.lower() }}
{%- else -%} NULL
{%- endif %}

{%- endmacro %}