{% macro redshift__strategy__materialized_view__create(relation, sql) %}
    {% set %}
        materialized_view_name = str(relation)
        kwargs = dict()
    {% endset %}
    {{ redshift__db_api__materialized_view__create(materialized_view, sql, kwargs) }}
{% endmacro %}
