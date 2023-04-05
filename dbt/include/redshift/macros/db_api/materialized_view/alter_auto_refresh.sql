{% macro redshift__db_api__materialized_view__alter_auto_refresh(relation, auto_refresh) %}
    {{ return(proxy_view) }}
{% endmacro %}
