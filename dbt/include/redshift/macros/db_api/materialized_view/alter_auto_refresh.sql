{% macro redshift__db_api__materialized_view__alter_auto_refresh(materialized_view_name, auto_refresh) %}
    {% set %}
        proxy_view = {{ ref(materialized_view_name) }}
    {% endset %}
    {{ return(proxy_view) }}
{% endmacro %}
