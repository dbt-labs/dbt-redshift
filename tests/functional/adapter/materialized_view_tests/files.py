MY_SEED = """
id,value
1,100
2,200
3,300
""".strip()


MY_TABLE = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""


MY_VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""


MY_MATERIALIZED_VIEW_ON = """
{{ config(
    materialized='materialized_view',
    auto_refresh=True,
) }}
select * from {{ ref('my_seed') }}
"""


MY_MATERIALIZED_VIEW_OFF = """
{{ config(
    materialized='materialized_view',
    auto_refresh=False,
) }}
select * from {{ ref('my_seed') }}
"""


MACRO__LAST_REFRESH = """
{% macro redshift__test__last_refresh(schema, identifier) %}
    {% set _sql %}
    select starttime as last_refresh
    from svl_mv_refresh_status
    where schema_name = '{{ schema }}'
    and mv_name = '{{ identifier }}'
    ;
    {% endset %}
    {{ return(run_query(_sql)) }}
{% endmacro %}
"""
