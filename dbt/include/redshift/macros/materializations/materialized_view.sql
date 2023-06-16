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

        {% if configuration_changes.autorefresh.is_change %}
            {{ redshift__update_auto_refresh_on_materialized_view(relation, configuration_changes.autorefresh) }}
        {%- endif -%}

    {%- endif -%}

{% endmacro %}


{% macro redshift__get_create_materialized_view_as_sql(relation, sql) %}

    {% set relation_config = relation.get_materialized_view_from_runtime_config(config) %}

    create materialized view {{ relation_config.mv_name }}
        backup {% if relation_config.backup -%}yes{% else %}no{% endif %}
        diststyle {{ relation_config.dist.diststyle }}
        {% if relation_config.dist.distkey %}distkey ({{ relation_config.dist.distkey }}){% endif %}
        {% if relation_config.sort.sortkey %}sortkey ({{ ','.join(relation_config.sort.sortkey) }}){% endif %}
        auto refresh {% if relation_config.auto_refresh %}yes{% else %}no{% endif %}
    as (
        {{ relation_config.query }}
    );

{% endmacro %}


{% macro redshift__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ redshift__get_drop_relation_sql(existing_relation) }};
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}


{% macro redshift__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _existing_materialized_view = redshift__describe_materialized_view(existing_relation) %}
    {% set _configuration_changes = existing_relation.get_materialized_view_config_change_collection(_existing_materialized_view, new_config) %}
    {% do return(_configuration_changes) %}
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


{% macro redshift__describe_materialized_view(relation) %}
    {#-
        These need to be separate queries because redshift will not let you run queries
        against svv_table_info and pg_views in the same query. The same is true of svv_redshift_columns.
    -#}

    {%- set _show_materialized_view_sql -%}
        select
            tb.database as database_name,
            tb.schema as schema_name,
            tb.table as mv_name,
            tb.diststyle as dist,
            mv.autorefresh as autorefresh
        from svv_table_info tb
        left join stv_mv_info mv
            on mv.db_name = tb.database
            and mv.schema = tb.schema
            and mv.name = tb.table
        where tb.table = '{{ relation.identifier }}'
        and tb.schema = '{{ relation.schema }}'
        and tb.database = '{{ relation.database }}'
    {%- endset %}
    {% set _materialized_view = run_query(_show_materialized_view_sql) %}

    {%- set _show_materialized_view_query_sql -%}
        select
            '{{ relation.database }}' as database_name,
            vw.schemaname as schema_name,
            vw.viewname as mv_name,
            vw.definition as query
        from pg_views vw
        where vw.viewname = '{{ relation.identifier }}'
        and vw.schemaname = '{{ relation.schema }}'
        and vw.definition ilike '%create materialized view%'
    {%- endset %}
    {% set _query = run_query(_show_materialized_view_query_sql) %}

    {%- set _show_materialized_view_sortkeys_sql -%}
        select
            col.database_name,
            col.schema_name,
            col.table_name as mv_name,
            case when coalesce(col.sortkey,0) > 0 then col.column_name end as sortkey
        from svv_redshift_columns col
        where col.table_name = '{{ relation.identifier }}'
        and col.schema_name = '{{ relation.schema }}'
        and col.database_name = '{{ relation.database }}'
    {%- endset %}
    {% set _sortkey = run_query(_show_materialized_view_sortkeys_sql) %}

    {% do return({'materialized_view': _materialized_view, 'query': _query, 'sortkey': _sortkey}) %}

{% endmacro %}


{% macro redshift__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }}
{%- endmacro %}
