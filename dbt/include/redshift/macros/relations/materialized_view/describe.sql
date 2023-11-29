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
        -- svv_mv_info is queryable by Redshift Serverless, but stv_mv_info is not
        left join svv_mv_info mv
            on mv.database_name = tb.database
            and mv.schema_name = tb.schema
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
