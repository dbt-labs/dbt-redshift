{% macro redshift__alter_relation_comment(relation, comment) %}
  {% do return(postgres__alter_relation_comment(relation, comment)) %}
{% endmacro %}


{% macro redshift__alter_column_comment(relation, column_dict) %}
  {% do return(postgres__alter_column_comment(relation, column_dict)) %}
{% endmacro %}
