{% macro redshift__get_create_materialized_view_as_sql(relation, sql) %}

    {% set _backup = config.get('backup', validator=validation.any[boolean]) %}
    {% set backup_clause %}{% if _backup == false %}backup no{% endif %}{% endset %}

    {% set _auto_refresh = config.get('auto_refresh', validator=validation.any[boolean]) %}
    {% set auto_refresh_clause %}{% if auto_refresh == true %}auto refresh yes{% endif %}{% endset %}

    {% set _dist_key = config.get('dist', validator=validation.any[basestring]) %}
    {% set dist_clause = dist(_dist_key) %}

    {% set _sort_key = config.get('sort', validator=validation.any[list, basestring]) %}
    {% set _sort_type = config.get('sort_type',validator=validation.any['compound', 'interleaved']) %}
    {% set sort_clause = sort(_sort_type, _sort_key) %}

    create materialized view {{ materialized_view_name }}
        {{- backup_clause -}}
        {{- auto_refresh_clause -}}
        {{- dist_clause -}}
        {{- sort_clause -}}
    as (
        {{ sql }}
    );

{% endmacro %}


{% macro redshift__get_refresh_data_in_materialized_view_sql(relation) %}
    refresh materialized view {{ relation }};
{% endmacro %}


{% macro redshift__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}

    {% if existing_relation is not none %}
        drop materialized view {{ existing_relation }};
    {% endif %}

    {{ get_create_materialized_view_as_sql(relation, sql) }}

{% endmacro %}


{% macro redshift__get_alter_materialized_view_sql(relation, updates, sql) %}

    {% if 'sort' in updates.keys() or 'dist' in updates.keys() %}

        {{ get_replace_materialized_view_as_sql(relation, sql) }}

    {% elif 'auto_refresh' in updates.keys() %}

        alter materialized view {{ relation }}
            auto refresh {% if auto_refresh is true %}yes{% else %}no{% endif %}
        ;

    {% endif %}

{% endmacro %}
