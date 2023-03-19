{% macro redshift__get_relations () -%}
  {{ return(dbt.postgres_get_relations()) }}
{% endmacro %}
