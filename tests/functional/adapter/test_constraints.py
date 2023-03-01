import pytest
from dbt.tests.util import relation_from_name
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsColumnsEqual,
    BaseConstraintsRuntimeEnforcement
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

class TestRedshiftConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture
    def int_array_type(self):
        return "INTEGER_ARRAY"

    @pytest.fixture
    def string_array_type(self):
        return "TEXT_ARRAY"

class TestRedshiftConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        relation = relation_from_name(project.adapter, "my_model")
        tmp_relation = relation.incorporate(
            path={"identifier": relation.identifier + "__dbt_tmp"}
        )
        return _expected_sql_redshift.format(tmp_relation)
    
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['Cannot insert a NULL value into column id']
