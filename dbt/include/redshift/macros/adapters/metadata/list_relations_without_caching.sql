{% macro redshift__list_relations_without_caching(schema_relation) %}

  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
        table_catalog as database,
        table_name as name,
        table_schema as schema,
        'table' as type
    from information_schema.tables
    where table_schema ilike '{{ schema_relation.schema }}'
    and table_type = 'BASE TABLE'
    union all
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case
        when view_definition ilike '%create materialized view%'
          then 'materialized_view'
        else 'view'
      end as type
    from information_schema.views
    where table_schema ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
