{% macro redshift__create_materialized_view(relation, sql) %}
    {% set proxy_view = redshift__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}


{% macro redshift__alter_materialized_view(relation, sql) %}
    {% set proxy_view = redshift__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}


{% macro redshift__refresh_materialized_view(relation, sql) %}
    {{ return({'relations': [relation]}) }}
{% endmacro %}