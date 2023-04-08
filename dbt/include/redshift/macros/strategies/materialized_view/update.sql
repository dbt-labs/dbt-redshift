{% macro redshift__strategy__materialized_view__update(relation, updates, sql=None) %}
    {% if 'sort_key' in updates.keys() or 'dist_key' in updates.keys() %}
        {{ redshift__strategy__materialized_view__full_refresh(relation, sql) }}
    {% elif 'auto_refresh' in updates.keys() %}
        {% set %}
            materialied_view_name = str(relation)
            auto_refresh = updates.get('auto_refresh')
        {% endset %}
        {{ redshift__db_api__materialized_view__alter_auto_refresh(materialized_view, auto_refresh) }}
    {% endif %}
{% endmacro %}
