{% macro redshift__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set materialized_view = adapter.relation_config_factory.make_from_node(config.model) -%}

    create materialized view {{ materialized_view.path }}
        backup {% if materialized_view.backup %}yes{% else %}no{% endif %}
        diststyle {{ materialized_view.dist.diststyle }}
        {% if materialized_view.dist.distkey %}distkey ({{ materialized_view.dist.distkey }}){% endif %}
        {% if materialized_view.sort.sortkey %}sortkey ({{ ','.join(materialized_view.sort.sortkey) }}){% endif %}
        auto refresh {% if materialized_view.autorefresh %}yes{% else %}no{% endif %}
    as (
        {{ materialized_view.query }}
    );

{% endmacro %}
