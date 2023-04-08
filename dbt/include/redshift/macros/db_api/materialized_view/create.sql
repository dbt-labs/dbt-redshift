{% macro redshift__db_api__materialized_view__create(materialized_view_name, sql, kwargs) %}

    {% set %}
        {% if 'backup' in kwargs %}
            backup_clause = {% redshift__db_api__utils__backup_clause(kwargs.get('backup')) %}
        {% else %}
            backup_clause = ''
        {% endif %}
        {% if 'auto_refresh' in kwargs %}
            auto_refresh_clause = {% redshift__db_api__utils__auto_refresh_clause(kwargs.get('auto_refresh')) %}
        {% else %}
            auto_refresh_clause = ''
        {% endif %}
        {% if 'dist_style' in kwargs %}
            dist_clause = {% redshift__db_api__utils__dist_clause(kwargs.get('dist_style'), kwargs.get('dist_key', '')) %}
        {% else %}
            dist_clause = ''
        {% endif %}
        {% if 'sort_type' in kwargs %}
            sort_clause = {% redshift__db_api__utils__sort_clause(kwargs.get('sort_type'), kwargs.get('sort_key', '')) %}
        {% else %}
            sort_clause = ''
        {% endif %}
    {% endset %}

    create materialized view {{ materialized_view_name }}
        {{- backup_clause -}}
        {{- auto_refresh_clause -}}
        {{- dist_clause -}}
        {{- sort_clause -}}
    as (
        {{ sql }}
    );

{% endmacro %}
