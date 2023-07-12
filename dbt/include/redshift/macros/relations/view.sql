{%- macro redshift__drop_view_template(view) -%}
    drop view if exists {{ view.fully_qualified_path }} cascade
{%- endmacro -%}


{# /* Redshift uses `table` here even for a view. Replacing this with `view` will break. */ #}
{%- macro redshift__rename_view_template(view, new_name) -%}
    alter table {{ view.fully_qualified_path }} rename to {{ new_name }}
{%- endmacro -%}


{# /*
    These are `BaseRelation` versions. The `BaseRelation` workflows are different.
*/ #}
{% macro redshift__create_view_as(relation, sql) -%}
  {%- set binding = config.get('bind', default=True) -%}

  {% set bind_qualifier = '' if binding else 'with no schema binding' %}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create view {{ relation }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %} as (
    {{ sql }}
  ) {{ bind_qualifier }};
{% endmacro %}
