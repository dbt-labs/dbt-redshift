{% macro redshift__create_table_as(temporary, relation, sql) %}

    {% set table_name = relation.include(database=(not temporary), schema=(not temporary)) %}
    {% set contract = config.get('contract') %}
    {% set dist_key = config.get('dist') %}
    {% set sort_type = config.get('sort_type', validator=validation.any['compound', 'interleaved']) %}
    {% set sort_key = config.get('sort', validator=validation.any[list, basestring]) %}
    {% set backup = config.get('backup') %}
    {% set sql_header = config.get('sql_header', none) %}

    {{ sql_header if sql_header is not none }}

    {% if contract.enforced %}
        {{ create_table_as_with_contract(temporary, table_name, sql, backup, dist_key, sort_type, sort_key) }}
    {% else %}
        {{ create_table_as_with_no_contract(temporary, table_name, sql, backup, dist_key, sort_type, sort_key) }}
    {% endif %}
{% endmacro %}


{% macro create_table_as_with_contract(temporary, table_name, sql, backup, dist_key, sort_type, sort_key) %}

    {{ get_assert_columns_equivalent(sql) }}

    create {% if temporary %}temporary{% endif %} table {{ table_name }}
        {{ get_columns_spec_ddl() }}
        {% if backup == false %}backup no{% endif %}
        {{ dist(dist_key) }}
        {{ sort(sort_type, sort_key) }}
    ;

    insert into {{ table_name }}
    (
        {{ get_select_subquery(sql) }}
    );

{% endmacro %}


{% macro create_table_as_with_no_contract(temporary, table_name, sql, backup, dist_key, sort_type, sort_key) %}

    create {% if temporary %}temporary{% endif %} table {{ table_name }}
        {% if backup == false %}backup no{% endif %}
        {{ dist(dist_key) }}
        {{ sort(sort_type, sort_key) }}
    as (
        {{ sql }}
    );

{% endmacro %}


{% macro dist(dist_key) %}
    {% if dist_key is not none %}
        {% set dist_key = dist_key.strip().lower() %}

        {% if dist_key in ['all', 'even'] %}
            diststyle {{ dist_key }}
        {% elif dist_key == "auto" %}
        {% else %}
            diststyle key distkey ({{ dist_key }})
        {% endif %}

  {% endif %}
{% endmacro %}


{% macro sort(sort_type, sort_key) %}
    {% if sort_key is not none %}
        {% if sort_key is string %}
            {% set sort_key = [sort_key] %}
        {% endif %}

        {{ sort_type | default('compound', boolean=true) }} sortkey(
            {% for item in sort_key %}
                {{ item }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
    {% endif %}
{% endmacro %}
