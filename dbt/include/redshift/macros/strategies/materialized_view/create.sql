{% macro redshift__strategy__materialized_view__create(relation, sql) %}
    {{ redshift__db_api__materialized_view__create(relation, sql) }}
{% endmacro %}
