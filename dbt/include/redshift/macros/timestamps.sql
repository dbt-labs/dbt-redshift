
{% macro redshift__current_timestamp() -%}
  getdate()
{%- endmacro %}

{% macro redshift__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp
{%- endmacro %}

{% macro redshift__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamp" -%}
    {{ return(result) }}
{%- endmacro %}

{%- macro redshiftt__convert_timezone(source_tz, target_tz, timestamp) -%}
{# See: https://docs.aws.amazon.com/redshift/latest/dg/CONVERT_TIMEZONE.html #}
    {%- if not source_tz -%}
        CONVERT_TIMEZONE({{target_tz}}, {{timestamp}})
    {%- else -%}
        CONVERT_TIMEZONE({{source_tz}}, {{target_tz}}, {{timestamp}})
    {%- endif -%}
{%- endmacro -%}
