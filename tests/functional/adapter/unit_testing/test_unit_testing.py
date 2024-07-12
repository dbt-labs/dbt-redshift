import pytest

from dbt.exceptions import ParsingError
from dbt.tests.fixtures.project import write_project_files
from dbt.tests.util import run_dbt

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from tests.functional.adapter.unit_testing.fixtures import (
    model_none_value_base,
    model_none_value_model,
    test_none_column_value_doesnt_throw_error_csv,
    test_none_column_value_doesnt_throw_error_dct,
    test_none_column_value_will_throw_error,
)


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


class RedshiftUnitTestingNone:
    def test_nones_handled_dict(self, project):
        run_dbt(["build"])


class TestRedshiftUnitTestCsvNone(RedshiftUnitTestingNone):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "none_value_base.sql": model_none_value_base,
            "none_value_model.sql": model_none_value_model,
            "__properties.yml": test_none_column_value_doesnt_throw_error_csv,
        }


class TestRedshiftUnitTestDictNone(RedshiftUnitTestingNone):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "none_value_base.sql": model_none_value_base,
            "none_value_model.sql": model_none_value_model,
            "__properties.yml": test_none_column_value_doesnt_throw_error_dct,
        }


class TestRedshiftUnitTestingTooManyNonesFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "__properties.yml": test_none_column_value_will_throw_error,
            "none_value_base.sql": model_none_value_base,
            "none_value_model.sql": model_none_value_model,
        }

    def test_invalid_input(self, project):
        with pytest.raises(ParsingError) as e:
            run_dbt(["build"])

        assert "Unit Test fixtures require at least one row free of None" in str(e)


class TestRedshiftUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestRedshiftUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
