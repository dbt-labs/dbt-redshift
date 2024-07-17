import pytest
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


class TestRedshiftUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["1.0", "1.0"],
            ["'1'", "1"],
            ["'1'::numeric", "1"],
            ["'string'", "string"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["TIMESTAMPTZ '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            [
                """JSON_PARSE('{"bar": "baz", "balance": 7.77, "active": false}')""",
                """'{"bar": "baz", "balance": 7.77, "active": false}'""",
            ],
            # TODO: array types
            # ["ARRAY[1,2,3]", """'{1, 2, 3}'"""],
            # ["ARRAY[1.0,2.0,3.0]", """'{1.0, 2.0, 3.0}'"""],
            # ["ARRAY[1::numeric,2::numeric,3::numeric]", """'{1.0, 2.0, 3.0}'"""],
            # ["ARRAY['a','b','c']", """'{"a", "b", "c"}'"""],
            # ["ARRAY[true,true,false]", """'{true, true, false}'"""],
            # ["ARRAY[DATE '2020-01-02']", """'{"2020-01-02"}'"""],
            # ["ARRAY[TIMESTAMP '2013-11-03 00:00:00-0']", """'{"2013-11-03 00:00:00-0"}'"""],
            # ["ARRAY[TIMESTAMPTZ '2013-11-03 00:00:00-0']", """'{"2013-11-03 00:00:00-0"}'"""],
        ]


class TestRedshiftUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestRedshiftUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
