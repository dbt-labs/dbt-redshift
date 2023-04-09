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

    {% set %}
        backup_clause = {% if backup is false %}backup no{% else %}{% endif %}
        auto_refresh_clause = {% if auto_refresh is true %}auto refresh yes{% else %}{% endif %}
        dist_clause = {% redshift__db_api__utils__dist_clause(dist_style, dist_key) %}
        sort_clause = {% redshift__db_api__utils__sort_clause(sort_type, sort_key) %}
    {% endset %}

    create materialized view {{ materialized_view_name }}
        {{- backup_clause -}}
        {{- auto_refresh_clause -}}
        {{- dist_clause -}}
        {{- sort_clause -}}
    as (
        {{ sql }}
    );

{% endmacro %}
