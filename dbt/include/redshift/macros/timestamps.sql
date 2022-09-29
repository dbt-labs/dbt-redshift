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
