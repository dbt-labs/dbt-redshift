{% macro redshift__strategy__materialized_view__refresh_data(relation) %}
    {% set %}
        materialized_view_name = str(relation)
    {% endset %}
    {{ redshift__db_api__materialized_view__refresh(materialized_view_name) }}
{% endmacro %}
