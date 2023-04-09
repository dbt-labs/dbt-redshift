{% macro redshift__strategy__materialized_view__create(relation, sql) %}

    {% set dist = config.get('dist', '') %}
    {% if dist == '' %}
        {% set dist_style = none %}
        {% set dist_key = none %}
    {% if dist in ['all', 'even', 'auto'] %}
        {% set dist_style = dist %}
        {% set dist_key = none %}
    {% else %}
        {% set dist_style = 'key' %}
        {% set dist_key = dist %}
    {% endif %}

    {{ redshift__db_api__materialized_view__create(
        materialized_view_name = str(relation),
        sql = sql,
        backup = config.get('backup', true),
        autorefresh = config.get('auto_refresh', false),
        dist_style = dist_style,
        dist_key = dist_key,
        sort_type = config.get('sort_type', none),
        sort_key = config.get('sort', none)
    ) }}

{% endmacro %}
