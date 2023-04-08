{% macro redshift__strategy__materialized_view__create(relation, sql) %}
    {% set materialized_view_name = str(relation) %}

    {% set kwargs = dict() %}
        {% if 'backup' in config %}
            {% set _dummy = kwargs.update({'backup': config.get('backup')}) %}
        {% endif %}
        {% if 'auto_refresh' in config %}
            {% set _dummy = kwargs.update({'auto_refresh': config.get('auto_refresh')}) %}
        {% endif %}
        {% if 'dist' in config %}
            {% set dist = config.get('dist') %}
            {% if dist in ['all', 'even', 'auto'] %}
                {% set _dummy = kwargs.update({'dist_style': dist}) %}
            {% else %}
                {% set _dummy = kwargs.update({'dist_style': 'key', 'dist_key': dist}) %}
            {% endif %}
        {% endif %}
        {% if 'sort_key' in config %}
            {% set _dummy = kwargs.update({'sort_type': config.get('sort_type', ''), 'sort_key': config.get('sort')}) %}
        {% endif %}

    {{ redshift__db_api__materialized_view__create(materialized_view, sql, kwargs) }}
{% endmacro %}
