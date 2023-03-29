{% macro redshift__create_view_as(relation, sql) %}

    {% set contract_config = config.get('contract') %}
    {% set binding = config.get('bind', true) %}
    {% set bind_qualifier = '' if binding else 'with no schema binding' %}
    {% set sql_header = config.get('sql_header', none) %}

    {% if contract_config.enforced %}
        {{ get_assert_columns_equivalent(sql) }}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    create view {{ relation }} as (
        {{ sql }}
    )
    {{ bind_qualifier }};

{% endmacro %}
