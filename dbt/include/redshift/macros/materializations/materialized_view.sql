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

        {%- set autorefresh = configuration_changes.autorefresh -%}
        {%- if autorefresh -%}{{- log('Applying UPDATE AUTOREFRESH to: ' ~ relation) -}}{%- endif -%}

        alter materialized view {{ relation }}
            auto refresh {% if autorefresh.context %}yes{% else %}no{% endif %}

    {%- endif -%}

{% endmacro %}


{% macro redshift__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set materialized_view = relation.from_runtime_config(config) -%}

    create materialized view {{ materialized_view.path }}
        backup {% if materialized_view.backup %}yes{% else %}no{% endif %}
        diststyle {{ materialized_view.dist.diststyle }}
        {% if materialized_view.dist.distkey %}distkey ({{ materialized_view.dist.distkey }}){% endif %}
        {% if materialized_view.sort.sortkey %}sortkey ({{ ','.join(materialized_view.sort.sortkey) }}){% endif %}
        auto refresh {% if materialized_view.auto_refresh %}yes{% else %}no{% endif %}
    as (
        {{ materialized_view.query }}
    );

{% endmacro %}


{% macro redshift__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ redshift__get_drop_relation_sql(existing_relation) }};
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}


{% macro redshift__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _existing_materialized_view = redshift__describe_materialized_view(existing_relation) %}
    {% set _configuration_changes = existing_relation.materialized_view_config_changeset(_existing_materialized_view, new_config) %}
    {% do return(_configuration_changes) %}
{% endmacro %}


{% macro redshift__refresh_materialized_view(relation) -%}
    refresh materialized view {{ relation }}
{% endmacro %}


{% macro redshift__describe_materialized_view(relation) %}
    {#-
        These need to be separate queries because redshift will not let you run queries
        against svv_table_info and pg_views in the same query. The same is true of svv_redshift_columns.
    -#}

    {%- set _materialized_view_sql -%}
        select
            tb.database,
            tb.schema,
            tb.table,
            tb.diststyle,
            tb.sortkey1,
            mv.autorefresh
        from svv_table_info tb
        left join stv_mv_info mv
            on mv.db_name = tb.database
            and mv.schema = tb.schema
            and mv.name = tb.table
        where tb.table ilike '{{ relation.identifier }}'
        and tb.schema ilike '{{ relation.schema }}'
        and tb.database ilike '{{ relation.database }}'
    {%- endset %}
    {% set _materialized_view = run_query(_materialized_view_sql) %}

    {%- set _query_sql -%}
        select
            vw.definition
        from pg_views vw
        where vw.viewname = '{{ relation.identifier }}'
        and vw.schemaname = '{{ relation.schema }}'
        and vw.definition ilike '%create materialized view%'
    {%- endset %}
    {% set _query = run_query(_query_sql) %}

    {% do return({'materialized_view': _materialized_view, 'query': _query}) %}

{% endmacro %}


{% macro redshift__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }}
{%- endmacro %}
