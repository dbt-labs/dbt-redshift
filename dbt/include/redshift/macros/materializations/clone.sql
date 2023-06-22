{% macro redshift__can_clone_table() %}
    {{ can_clone_table() }}
{% endmacro %}
