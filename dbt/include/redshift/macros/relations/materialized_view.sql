{#- /*
    This file contains DDL that gets consumed in the default materialized view materialization in `dbt-core`.
    These macros could be used elsewhere as they do not care that they are being called by a materialization;
    but the original intention was to support the materialization of materialized views. These macros represent
    the basic interactions dbt-postgres requires of materialized views in Postgres:
        - ALTER
        - CREATE
        - DESCRIBE
        - DROP
        - REFRESH
        - RENAME
    These macros all take a `RedshiftMaterializedViewRelation` instance as an input. This class can be found in:
        `dbt/adapters/redshift/relation/models/materialized_view.py`

    Used in:
        `dbt/include/global_project/macros/materializations/models/materialized_view/materialized_view.sql`
    Uses:
        `dbt/adapters/redshift/relation/factory.py`
*/ -#}

{% macro redshift__alter_materialized_view_template(existing_materialized_view, target_materialized_view) %}

    {%- if target_materialized_view == existing_materialized_view -%}
        {{- exceptions.warn("No changes were identified for: " ~ existing_materialized_view) -}}

    {%- else -%}
        {%- set _changeset = adapter.make_changeset(existing_materialized_view, target_materialized_view) -%}

        {% if _changeset.requires_full_refresh %}
            {{ replace_template(existing_materialized_view, target_materialized_view) }}

        {% else %}

            {%- set autorefresh = _changeset.autorefresh -%}
            {%- if autorefresh -%}
                {{- log('Applying UPDATE AUTOREFRESH to: ' ~ existing_materialized_view) -}}
                alter materialized view {{ existing_materialized_view.fully_qualified_path }}
                    auto refresh {% if autorefresh.context %}yes{% else %}no{% endif %}
            {%- endif -%}

        {%- endif -%}
    {%- endif -%}

{% endmacro %}


{% macro redshift__create_materialized_view_template(materialized_view) %}

    create materialized view {{ materialized_view.fully_qualified_path }}
        backup {% if materialized_view.backup %}yes{% else %}no{% endif %}
        diststyle {{ materialized_view.dist.diststyle }}
        {% if materialized_view.dist.distkey %}distkey ({{ materialized_view.dist.distkey }}){% endif %}
        {% if materialized_view.sort.sortkey %}sortkey ({{ ','.join(materialized_view.sort.sortkey) }}){% endif %}
        auto refresh {% if materialized_view.autorefresh %}yes{% else %}no{% endif %}
    as (
        {{ materialized_view.query }}
    )

{% endmacro %}


{% macro redshift__describe_materialized_view_template(materialized_view) %}
    {#-
        These need to be separate queries because redshift will not let you run queries
        against svv_table_info and pg_views in the same query. The same is true of svv_redshift_columns.
    -#}

    {%- set _materialized_view_sql -%}
        select
            tb.database as database_name,
            tb.schema as schema_name,
            tb.table as name,
            tb.diststyle as dist,
            tb.sortkey1 as sortkey,
            mv.autorefresh
        from svv_table_info tb
        left join stv_mv_info mv
            on mv.db_name = tb.database
            and mv.schema = tb.schema
            and mv.name = tb.table
        where tb.table ilike '{{ materialized_view.name }}'
        and tb.schema ilike '{{ materialized_view.schema_name }}'
        and tb.database ilike '{{ materialized_view.database_name }}'
    {%- endset %}
    {% set _materialized_view = run_query(_materialized_view_sql) %}

    {%- set _query_sql -%}
        select
            vw.definition as query
        from pg_views vw
        where vw.viewname = '{{ materialized_view.name }}'
        and vw.schemaname = '{{ materialized_view.schema_name }}'
        and vw.definition ilike '%create materialized view%'
    {%- endset %}
    {% set _query = run_query(_query_sql) %}

    {% do return({'materialized_view': _materialized_view, 'query': _query}) %}

{% endmacro %}


{% macro redshift__drop_materialized_view_template(materialized_view) -%}
    drop materialized view if exists {{ materialized_view.fully_qualified_path }}
{%- endmacro %}


{% macro redshift__refresh_materialized_view_template(materialized_view) -%}
    refresh materialized view {{ materialized_view.fully_qualified_path }}
{% endmacro %}


{%- macro redshift__rename_materialized_view_template(materialized_view, new_name) -%}
    {{- exceptions.raise_compiler_error(
        "Redshift does not support the renaming of materialized views. This macro was called by: " ~ materialized_view
    ) -}}
{%- endmacro -%}
