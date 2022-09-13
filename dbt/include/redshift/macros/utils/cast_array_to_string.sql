{# redshift should use default instead of postgres #}
{% macro redshift__cast_array_to_string(array) %}
    cast({{ array }} as {{ type_string() }})
{% endmacro %}
