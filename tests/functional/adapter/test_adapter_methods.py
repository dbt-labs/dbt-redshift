import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.fixtures.project import write_project_files

models__upstream_sql = """
select 1 as id

"""

models__expected_sql = """

select 2 as id

"""

models__invalid_schema_model = """

{% set upstream = ref('upstream') %}

{% set existing = adapter.check_schema_exists(upstream.database, "doesnotexist") %}
{% if existing == False %}
select 2 as id
{% else %}
select 1 as id
{% endif %}

"""

models__valid_schema_model = """

{% set upstream = ref('upstream') %}

{% set existing = adapter.check_schema_exists(upstream.database, upstream.schema) %}
{% if existing == True %}
select 2 as id
{% else %}
select 1 as id
{% endif %}

"""


class BaseAdapterMethod:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": models__upstream_sql,
            "expected.sql": models__expected_sql,
            "invalid_schema.sql": models__invalid_schema_model,
            "valid_schema.sql": models__valid_schema_model,
        }

    @pytest.fixture(scope="class")
    def project_files(
        self,
        project_root,
        tests,
        models,
    ):
        write_project_files(project_root, "models", models)

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "adapter_methods",
        }

    # snowflake need all tables in CAP name
    @pytest.fixture(scope="class")
    def equal_tables(self):
        return ["invalid_schema", "expected"]

    @pytest.fixture(scope="class")
    def equal_tables2(self):
        return ["valid_schema", "expected"]

    def test_adapter_methods(self, project, equal_tables):
        run_dbt(["compile"])  # trigger any compile-time issues
        result = run_dbt()
        assert len(result) == 4
        check_relations_equal(project.adapter, equal_tables)
        check_relations_equal(project.adapter, ["valid_schema", "expected"])


class TestBaseCaching(BaseAdapterMethod):
    pass
