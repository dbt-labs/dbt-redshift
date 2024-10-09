from dbt.adapters.base import Column
from dbt.tests.util import run_dbt
import pytest

from dbt.adapters.redshift import RedshiftRelation


class ColumnsInRelation:

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1.23 as my_num, 'a' as my_char"}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    def test_columns_in_relation(self, project):
        my_relation = RedshiftRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="my_model",
            type=RedshiftRelation.View,
        )
        with project.adapter.connection_named("_test"):
            actual_columns = project.adapter.get_columns_in_relation(my_relation)
        expected_columns = [
            Column(column="my_num", dtype="numeric", numeric_precision=3, numeric_scale=2),
            Column(column="my_char", dtype="character varying", char_size=1),
        ]
        assert actual_columns == expected_columns


class TestColumnsInRelationBehaviorFlagOff(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {}}


class TestColumnsInRelationBehaviorFlagOn(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"restrict_direct_pg_catalog_access": True}}
