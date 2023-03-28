{% macro redshift__create_table_as(temporary, relation, sql) %}

    {% set table_name = relation.include(database=(not temporary), schema=(not temporary)) %}
    {% set contract = config.get('contract') %}
    {% set dist = config.get('dist') %}
    {% set sort_type = config.get('sort_type', validator=validation.any['compound', 'interleaved']) %}
    {% set sort = config.get('sort', validator=validation.any[list, basestring]) %}
    {% set backup = config.get('backup') %}
    {% set sql_header = config.get('sql_header', none) %}

    {{ _sql_header if _sql_header is not none }}

    {% if contract.enforced %}
        {{ redshift__create_table_as_with_contract(temporary, table_name, sql, backup, dist, sort_type, sort) }}
    {% else %}
        {{ redshift__create_table_as_with_no_contract(temporary, table_name, sql, backup, dist, sort_type, sort) }}
    {% endif %}
{% endmacro %}


{% macro redshift__create_table_as_with_contract(temporary, table_name, sql, backup, dist, sort_type, sort) %}

    {{ get_assert_columns_equivalent(sql) }}

    create {% if temporary %}temporary{% endif %} table {{ table_name }}
        {{ get_columns_spec_ddl() }}
        {% if backup == false %}backup no{% endif %}
        {{ dist(dist) }}
        {{ sort(sort_type, sort) }}
    ;

    insert into {{ table_name }}
    (
        {{ get_select_subquery(sql) }}
    );

{% endmacro %}


{% macro redshift__create_table_as_with_no_contract(temporary, table_name, sql, backup, dist, sort_type, sort) %}

    create {% if temporary %}temporary{% endif %} table {{ table_name }}
        {% if backup == false %}backup no{% endif %}
        {{ dist(dist) }}
        {{ sort(sort_type, sort) }}
    as (
        {{ sql }}
    );

{% endmacro %}
