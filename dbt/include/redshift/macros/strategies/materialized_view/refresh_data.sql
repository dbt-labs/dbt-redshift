{% macro redshift__strategy__materialized_view__refresh_data(relation) %}
    {{ redshift__db_api__materialized_view__refresh(relation) }}
{% endmacro %}
