{% macro redshift__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ redshift__get_drop_relation_sql(existing_relation) }};
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}
