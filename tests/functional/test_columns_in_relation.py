from dbt.tests.util import get_connection, run_dbt
import pytest


MY_CROSS_DB_SOURCES = """
version: 2
sources:
  - name: ci
    schema: adapter
    tables:
      - name: cross_db
  - name: ci_alt
    database: ci_alt
    schema: adapter
    tables:
      - name: cross_db
"""


class TestCrossDatabase:
    """
    This addresses https://github.com/dbt-labs/dbt-redshift/issues/736
    """

    @pytest.fixture(scope="class")
    def models(self):
        my_model = """
        select '{{ adapter.get_columns_in_relation(source('ci', 'cross_db')) }}' as columns
        union all
        select '{{ adapter.get_columns_in_relation(source('ci_alt', 'cross_db')) }}' as columns
        """
        return {
            "sources.yml": MY_CROSS_DB_SOURCES,
            "my_model.sql": my_model,
        }

    def test_columns_in_relation(self, project):
        run_dbt(["run"])
        with get_connection(project.adapter, "_test"):
            records = project.run_sql(f"select * from {project.test_schema}.my_model", fetch=True)
        assert len(records) == 2
