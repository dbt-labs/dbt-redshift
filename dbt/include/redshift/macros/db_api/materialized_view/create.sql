{% macro redshift__db_api__materialized_view__create(
    materialized_view_name,
    sql,
    backup,
    auto_refresh,
    dist_style,
    dist_key,
    sort_type,
    sort_key
) %}
    {% set relation = {{ ref(materialized_view_name) }} %}
    {% set proxy_view = redshift__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}
