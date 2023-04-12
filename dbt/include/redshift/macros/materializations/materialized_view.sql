{% macro redshift__get_create_materialized_view_as_sql(relation, sql) %}

    {% set dist_clause = dist(config.get('dist', none)) %}

    {% set _sort_key = config.get('sort', validator=validation.any[list, basestring]) %}
    {% set _sort_type = config.get('sort_type',validator=validation.any['compound', 'interleaved']) %}
    {% set sort_clause = sort(_sort_type, _sort_key) %}

    {% set proxy_view = create_view_as(relation, sql) %}
    {{ return(proxy_view) }}

{% endmacro %}


{% macro redshift__get_refresh_data_in_materialized_view_sql(relation) %}
    select 1;
{% endmacro %}


{% macro default__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}

    {% if existing_relation is not none %}
        drop view {{ existing_relation }};
    {% endif %}

    {{ get_create_materialized_view_as_sql(relation, sql) }}

{% endmacro %}
