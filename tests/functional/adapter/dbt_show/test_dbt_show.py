from dbt.tests.adapter.dbt_show.test_dbt_show import BaseShowSqlHeader, BaseShowLimit


class TestRedshiftShowLimit(BaseShowLimit):
    pass


class TestRedshiftShowSqlHeader(BaseShowSqlHeader):
    pass
