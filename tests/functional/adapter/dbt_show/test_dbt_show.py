import pytest
from dbt.tests.adapter.dbt_show.test_dbt_show import BaseShowSqlHeader, BaseShowLimit


my_model_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}
{% call set_sql_header(config) %}
CREATE TEMPORARY TABLE _variables AS (
    SELECT '89' as my_variable
);
{% endcall %}
SELECT my_variable from _variables
"""


class TestRedshiftShowLimit(BaseShowLimit):
    pass


class TestRedshiftShowSqlHeader(BaseShowSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sql_header.sql": my_model_sql_header_sql,
        }
