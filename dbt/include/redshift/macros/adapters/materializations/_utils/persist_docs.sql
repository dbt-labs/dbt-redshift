{% macro redshift__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {# Override: do not set column comments for LBVs #}
  {% set is_lbv = config.get('materialized') == 'view' and config.get('bind') == false %}
  {% if for_columns and config.persist_column_docs() and model.columns and not is_lbv %}
    {% do run_query(alter_column_comment(relation, model.columns)) %}
  {% endif %}
{% endmacro %}
