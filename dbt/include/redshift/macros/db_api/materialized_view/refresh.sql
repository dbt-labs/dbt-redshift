{% macro redshift__db_api__materialized_view__refresh(materialized_view_name) %}
    refresh materialized view {{ materialized_view_name }};
{% endmacro %}
