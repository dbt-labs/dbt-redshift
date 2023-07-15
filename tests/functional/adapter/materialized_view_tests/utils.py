from typing import Optional

from dbt.adapters.base.relation import BaseRelation

from dbt.adapters.redshift.relation import RedshiftRelation


def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
    assert isinstance(relation, RedshiftRelation)
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


def query_sort(project, relation: RedshiftRelation) -> str:
    sql = f"""
        select
            tb.sortkey1 as sortkey
        from svv_table_info tb
        where tb.table ilike '{ relation.identifier }'
        and tb.schema ilike '{ relation.schema }'
        and tb.database ilike '{ relation.database }'
    """
    return project.run_sql(sql, fetch="one")[0]


def query_dist(project, relation: RedshiftRelation) -> str:
    sql = f"""
        select
            tb.diststyle
        from svv_table_info tb
        where tb.table ilike '{ relation.identifier }'
        and tb.schema ilike '{ relation.schema }'
        and tb.database ilike '{ relation.database }'
    """
    return project.run_sql(sql, fetch="one")[0]


def query_autorefresh(project, relation: RedshiftRelation) -> bool:
    sql = f"""
        select
            case mv.autorefresh when 't' then True when 'f' then False end as autorefresh
        from stv_mv_info mv
        where trim(mv.name) ilike '{ relation.identifier }'
        and trim(mv.schema) ilike '{ relation.schema }'
        and trim(mv.db_name) ilike '{ relation.database }'
    """
    return project.run_sql(sql, fetch="one")[0]
