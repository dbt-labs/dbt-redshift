{% macro redshift__get_alter_materialized_view_sql(relation, updates, sql) %}
    {% if 'sort' in updates.keys() or 'dist' in updates.keys() %}
        {{ get_replace_materialized_view_as_sql(relation, sql) }}
    {% elif 'auto_refresh' in updates.keys() %}
        select 1;
    {% endif %}
{% endmacro %}
