{% macro redshift__current_timestamp() %}
    getdate()
{% endmacro %}

{# redshift should use default instead of postgres #}
{% macro redshift__current_timestamp_in_utc() %}
    {{ return(dbt.default__current_timestamp_in_utc()) }}
{% endmacro %}
