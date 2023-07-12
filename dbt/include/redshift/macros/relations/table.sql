{%- macro redshift__drop_table_template(table) -%}
    drop table if exists {{ table.fully_qualified_path }} cascade
{%- endmacro -%}


{%- macro redshift__rename_table_template(table, new_name) -%}
    alter table {{ table.fully_qualified_path }} rename to {{ new_name }}
{%- endmacro -%}


{# /*
    These are `BaseRelation` versions. The `BaseRelation` workflows are different.
*/ #}

{% macro redshift__create_table_as(temporary, relation, sql) -%}

  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get(
          'sort_type',
          validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get(
          'sort',
          validator=validation.any[list, basestring]) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set backup = config.get('backup') -%}

  {{ sql_header if sql_header is not none }}

  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {{ get_table_columns_and_constraints() }}
    {{ get_assert_columns_equivalent(sql) }}
    {%- set sql = get_select_subquery(sql) %}
    {% if backup == false -%}backup no{%- endif %}
    {{ dist(_dist) }}
    {{ sort(_sort_type, _sort) }}
  ;

  insert into {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    (
      {{ sql }}
    )
  ;

  {%- else %}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if backup == false -%}backup no{%- endif %}
    {{ dist(_dist) }}
    {{ sort(_sort_type, _sort) }}
  as (
    {{ sql }}
  );

  {%- endif %}
{%- endmacro %}
