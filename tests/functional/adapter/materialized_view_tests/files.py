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


MY_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
) }}
select * from {{ ref('my_seed') }}
"""


MACRO__LAST_REFRESH = """
{% macro redshift__test__last_refresh(schema, identifier) %}
    {% set _sql %}

    {% endset %}
    {{ return(run_query(_sql)) }}
{% endmacro %}
"""
