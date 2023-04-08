{% macro redshift__db_api__materialized_view__create(materialized_view_name, sql, kwargs) %}
    {% set proxy_view = redshift__create_view_as(materialized_view_name, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}
