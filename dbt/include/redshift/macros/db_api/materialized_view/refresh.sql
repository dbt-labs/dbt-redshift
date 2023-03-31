{% macro redshift__db_api__materialized_view__refresh(relation) %}
    {{ return({'relations': [relation]}) }}
{% endmacro %}
