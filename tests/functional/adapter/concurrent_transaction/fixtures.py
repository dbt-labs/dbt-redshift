create_udfs_sql = """
{% macro create_udfs() %}

CREATE OR REPLACE FUNCTION {{ target.schema }}.f_sleep (x float)
RETURNS bool IMMUTABLE
AS
$$
  from time import sleep
  sleep(x)
  return True
$$ LANGUAGE plpythonu;

{% endmacro %}
""".lstrip()

#
# incremental materialization
#
incremental_model_sql = """
{{ config(materialized='incremental', unique_key='id') }}

-- incremental model
select 1 as id

{% if is_incremental() %}
    where TRUE
{% endif %}
""".lstrip()

#
# table materialization
#
table_model_sql = """
{{ config(materialized='table') }}

-- table model
select 1 as id
""".lstrip()

#
# view materialization
#
view_model_sql = """
{{ config(materialized='view') }}

-- view model
select 1 as id
""".lstrip()

#
# Common view model
#

view_sql = """
select * from {{ ref('model_1') }}
""".lstrip()
