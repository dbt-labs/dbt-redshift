from typing import Optional

from dbt.tests.util import get_model_file, set_model_file

from dbt.adapters.redshift.relation import RedshiftRelation


def swap_sortkey(project, my_materialized_view):
    initial_model = get_model_file(project, my_materialized_view)
    new_model = initial_model.replace("sort=['id']", "sort=['value']")
    set_model_file(project, my_materialized_view, new_model)


def swap_autorefresh(project, my_materialized_view):
    initial_model = get_model_file(project, my_materialized_view)
    new_model = initial_model.replace("dist='id'", "dist='id', auto_refresh=True")
    set_model_file(project, my_materialized_view, new_model)


def swap_materialized_view_to_table(project, my_materialized_view):
    initial_model = get_model_file(project, my_materialized_view)
    new_model = initial_model.replace("materialized='materialized_view'", "materialized='table'")
    set_model_file(project, my_materialized_view, new_model)


def swap_materialized_view_to_view(project, my_materialized_view):
    initial_model = get_model_file(project, my_materialized_view)
    new_model = initial_model.replace("materialized='materialized_view'", "materialized='view'")
    set_model_file(project, my_materialized_view, new_model)


def query_relation_type(project, relation: RedshiftRelation) -> Optional[str]:
    sql = f"""
    select
        'table' as relation_type
    from pg_tables
    where schemaname = '{relation.schema}'
    and tablename = '{relation.identifier}'
    union all
    select
        case
            when definition ilike '%create materialized view%'
            then 'materialized_view'
            else 'view'
        end as relation_type
    from pg_views
    where schemaname = '{relation.schema}'
    and viewname = '{relation.identifier}'
    """
    results = project.run_sql(sql, fetch="all")
    if len(results) == 0:
        return None
    elif len(results) > 1:
        raise ValueError(f"More than one instance of {relation.identifier} found!")
    else:
        return results[0][0]


def query_row_count(project, relation: RedshiftRelation) -> int:
    sql = f"select count(*) from {relation}"
    return project.run_sql(sql, fetch="one")[0]


def query_sort(project, relation: RedshiftRelation) -> bool:
    sql = f"""
        select
            tb.sortkey1 as sortkey
        from svv_table_info tb
        where tb.table ilike '{ relation.name }'
        and tb.schema ilike '{ relation.schema }'
        and tb.database ilike '{ relation.database }'
    """
    return project.run_sql(sql, fetch="one")[0]


def query_autorefresh(project, relation: RedshiftRelation) -> bool:
    sql = f"""
        select
            case mv.autorefresh when 't' then True when 'f' then False end as autorefresh
        from stv_mv_info mv
        where trim(mv.name) ilike '{ relation.name }'
        and trim(mv.schema) ilike '{ relation.schema }'
        and trim(mv.db_name) ilike '{ relation.database }'
    """
    return project.run_sql(sql, fetch="one")[0]
