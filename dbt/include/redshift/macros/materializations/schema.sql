{% macro redshift__create_schema(relation) %}
    {% if relation.database %}
        {{ adapter.verify_database(relation.database) }}
    {% endif %}

    {% call statement('create_schema') %}
        create schema if not exists {{ relation.without_identifier().include(database=False) }}
    {% endcall %}
{% endmacro %}


{% macro redshift__drop_schema(relation) %}
    {% if relation.database %}
        {{ adapter.verify_database(relation.database) }}
    {% endif %}

    {% call statement('drop_schema') %}
        drop schema if exists {{ relation.without_identifier().include(database=False) }} cascade
    {% endcall %}
{% endmacro %}
