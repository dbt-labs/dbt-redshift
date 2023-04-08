{% macro redshift__db_api__materialized_view__refresh(materialized_view_name) %}
    {% set %}
        relation = {{ ref(materialized_view_name) }}
    {% endset %}
    {{ return({'relations': [relation]}) }}
{% endmacro %}
