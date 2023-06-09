{% macro redshift__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    {{ get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) }}
{% endmacro %}


{% macro redshift__get_create_materialized_view_as_sql(relation, sql) %}

    {% set relation_config = relation.get_materialized_view_from_runtime_config(config) %}

    create materialized view {{ relation_config.mv_name }}
        backup {% if relation_config.backup -%}yes{% else %}no{% endif %}
        diststyle {{ relation_config.dist.diststyle }}
        {% if relation_config.dist.distkey %}distkey ({{ relation_config.dist.distkey }}){% endif %}
        {% if relation_config.sort.sortkey %}sortkey ({{ ','.join(relation_config.sort.sortkey) }}){% endif %}
        auto refresh {% if relation_config.auto_refresh %}yes{% else %}no{% endif %}
    as (
        {{ relation_config.query }}
    );

{% endmacro %}


{% macro redshift__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ redshift__get_drop_relation_sql(existing_relation) }};
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}


{% macro redshift__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% do return(None) %}
{% endmacro %}


{% macro redshift__refresh_materialized_view(relation) -%}
    refresh materialized view {{ relation }}
{% endmacro %}


{% macro redshift__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }}
{%- endmacro %}
