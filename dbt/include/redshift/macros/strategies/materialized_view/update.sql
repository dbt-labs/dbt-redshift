{% macro redshift__strategy__materialized_view__update(relation, updates, sql=None) %}
    {% if 'sort_key' in updates.keys() or 'dist_key' in updates.keys() %}
        {{ redshift__strategy__materialized_vew__full_refresh(relation, sql) }}
    {% elif 'auto_refresh' in updates.keys() %}
        {% set auto_refresh = updates.get('auto_refresh') %}
        {{ redshift__db_api__materialized_view__alter_auto_refresh(relation, auto_refresh) }}
    {% endif %}
{% endmacro %}
