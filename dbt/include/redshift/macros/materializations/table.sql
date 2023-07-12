{% /*

    Ideally we don't overwrite materializations from dbt-core. However, the implementation of materialized views
    requires this, at least for now. There are two issues that lead to this. First, Redshift does not support
    the renaming of materialized views. That means we cannot back them up when replacing them. If the relation
    that's replacing it is another materialized view, we can control for that since the materialization for
    materialized views in dbt-core is flexible. That brings us to the second issue. The materialization for table
    has the backup/deploy portion built into it; it's one single macro; replacing that has two options. We
    can either break apart the macro in dbt-core, which could have unintended downstream effects for all
    adapters. Or we can copy this here and keep it up to date with dbt-core until we resolve the larger issue.
    We chose to go with the latter.

*/ %}

{% materialization table, adapter='redshift', supported_languages=['sql'] %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
  {%- endcall %}

  -- cleanup -- this should be the only piece that differs from dbt-core
  {% if existing_relation is not none %}
      {% if existing_relation.type == 'materialized_view' %}
          {{ drop_relation_if_exists(existing_relation) }}
      {% else %}
          {{ adapter.rename_relation(existing_relation, backup_relation) }}
      {% endif %}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
