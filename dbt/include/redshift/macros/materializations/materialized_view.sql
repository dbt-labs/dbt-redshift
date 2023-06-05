{% macro redshift__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}

    -- apply a full refresh immediately if needed
    {% if configuration_changes.requires_full_refresh %}

        {{ get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) }}

    -- otherwise apply individual changes as needed
    {% else %}

        {% if configuration_changes.auto_refresh.is_change %}
            {{ redshift__update_auto_refresh_on_materialized_view(relation, configuration_changes.auto_refresh) }}
        {%- endif -%}

    {%- endif -%}

{% endmacro %}


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
    -- Redshift does not allow you to rename materialized views, so we cannot use `backup_relation` nor `intermediate_relation`
    -- TODO:
    -- We're not accounting for the scenario where the existing relation is not a materialized view (e.g. table)
    --    In this scenario, we could actually do the name swapping, but we have not implemented that.
    {{ drop_relation_if_exists(existing_relation) }}
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}


{% macro redshift__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {%- set _dist = dist(config.get('dist', none)) -%}
    {%- set _sort = config.get('sort', validator=validation.any[list, basestring]) -%}
    {%- set _sort_type = config.get('sort_type',validator=validation.any['compound', 'interleaved']) -%}
    {%- set backup = config.get('backup') -%}
    {%- set auto_refresh = 'yes' if config.get('auto_refresh', false) else 'no' %}

{% endmacro %}


{% macro redshift__refresh_materialized_view(relation) -%}
    refresh materialized view {{ relation }}
{% endmacro %}


{% macro redshift__update_auto_refresh_on_materialized_view(relation, auto_refresh_change) -%}
    {{- log("Applying UPDATE AUTO REFRESH to: " ~ relation) -}}

    {%- set _auto_refresh = auto_refresh_change.context -%}

    alter materialized view {{ relation }}
        auto refresh {% if _auto_refresh %}yes{% else %}no{% endif %}

{%- endmacro -%}

-- TODO
-- fix caching relation and get_relation
