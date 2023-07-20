{% macro redshift__get_drop_relation_sql(relation) %}
    {%- if relation.is_materialized_view -%}
        {{ redshift__drop_materialized_view(relation) }}
    {%- else -%}
        drop {{ relation.type }} if exists {{ relation }} cascade
    {%- endif -%}
{% endmacro %}
