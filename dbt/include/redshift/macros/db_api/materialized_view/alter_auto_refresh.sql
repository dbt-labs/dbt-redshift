{% macro redshift__db_api__materialized_view__alter_auto_refresh(materialized_view_name, auto_refresh) -%}
    alter materialized view {{ materialized_view_name }}
        auto refresh {% if auto_refresh is true %}yes{% else %}no{% endif %}
    ;
{% endmacro %}
