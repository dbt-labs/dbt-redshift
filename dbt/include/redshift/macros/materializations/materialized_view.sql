{% macro redshift__get_create_materialized_view_as_sql(relation, sql) %}

    create materialized view if not exists {{ relation }}
        {% if backup == false -%}backup no{%- endif %}
        {{ dist(_dist) }}
        {{ sort(_sort_type, _sort) }}
        auto refresh {{ auto_refresh }}
    as (
        {{ sql }}
    );

{% endmacro %}


{% macro redshift__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}

    {{ drop_relation_if_exists(existing_relation) }}

    {{ get_create_materialized_view_as_sql(relation, sql) }}

{% endmacro %}


{% macro redshift__refresh_materialized_view(relation) -%}
  {{ postgres__refresh_materialized_view(relation) }}
{% endmacro %}


{% macro redshift__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }};
{%- endmacro %}


{% macro redshift__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {%- set _dist = dist(config.get('dist', none)) -%}
    {%- set _sort = config.get('sort', validator=validation.any[list, basestring]) -%}
    {%- set _sort_type = config.get('sort_type',validator=validation.any['compound', 'interleaved']) -%}
    {%- set backup = config.get('backup') -%}
    {%- set auto_refresh = 'yes' if config.get('auto_refresh', false) else 'no' %}
{% endmacro %}


-- TODO
-- fix caching relation and get_relation
-- sort and dist key updates
-- \   if change in sort and dist keys trigger refresh
-- two new methods in impl
-- add alter method
