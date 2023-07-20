{% macro redshift__create_schema(relation) -%}
  {{ postgres__create_schema(relation) }}
{% endmacro %}


{% macro redshift__drop_schema(relation) -%}
  {{ postgres__drop_schema(relation) }}
{% endmacro %}


{% macro redshift__list_schemas(database) -%}
  {{ return(postgres__list_schemas(database)) }}
{%- endmacro %}

{% macro redshift__check_schema_exists(information_schema, schema) -%}
  {{ return(postgres__check_schema_exists(information_schema, schema)) }}
{%- endmacro %}
