import pytest
from dbt.tests.util import relation_from_name
from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseConstraintsRuntimeEnforcement,
)

_expected_sql_redshift = """
create table {0} (
    id integer not null,
    color text,
    date_day date,
    primary key(id)
) ;
insert into {0}
(
    select
        1 as id,
        'blue' as color,
        cast('2019-01-01' as date) as date_day
)
;
"""


class RedshiftColumnEqualSetup:
    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # NOTE: Unlike some other adapters, we don't test array or JSON types here, because
        # Redshift does not support them as materialized table column types.

        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "bool", "BOOL"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "timestamptz", "TIMESTAMPTZ"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "TIMESTAMP"],
            ["'1'::numeric", "numeric", "NUMERIC"],
        ]


class TestRedshiftTableConstraintsColumnsEqual(
    RedshiftColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass


class TestRedshiftViewConstraintsColumnsEqual(
    RedshiftColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    pass


class TestRedshiftConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        relation = relation_from_name(project.adapter, "my_model")
        tmp_relation = relation.incorporate(path={"identifier": relation.identifier + "__dbt_tmp"})
        return _expected_sql_redshift.format(tmp_relation)

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Cannot insert a NULL value into column id"]
