from base_grants import BaseGrantsRedshift
import pytest
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    write_file,
)

my_model_sql = """
  select 1 as fun
"""

model_schema_yml = """
version: 2
models:
  - name: my_model_view
    config:
      materialized: view
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_model_schema_yml = """
version: 2
models:
  - name: my_model_view
    config:
      materialized: view
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


class TestModelGrantsViewRedshift(BaseGrantsRedshift):
    @pytest.fixture(scope="class")
    def models(self):
        updated_schema = self.interpolate_name_overrides(model_schema_yml)
        return {
            "my_model_view.sql": my_model_sql,
            "schema.yml": updated_schema,
        }

    def test_view_table_grants(self, project, get_test_users, get_test_groups, get_test_roles):
        # Override/refactor the tests from dbt-core #
        # we want the test to fail, not silently skip
        test_users = get_test_users
        test_groups = get_test_groups
        test_roles = get_test_roles
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3
        assert len(test_groups) == 3
        assert len(test_roles) == 3

        # View materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_view"
        model = manifest.nodes[model_id]
        # user configuration for grants
        user_expected = {select_privilege_name: [test_users[0]]}
        assert model.config.grants == user_expected
        assert model.config.materialized == "view"
        # new configuration for grants
        expected = {select_privilege_name: {"user": [test_users[0]]}}

        actual_grants = self.get_grants_on_relation(project, "my_model_view")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # View materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1

        expected = {select_privilege_name: {"user": [test_users[1]]}}
        actual_grants = self.get_grants_on_relation(project, "my_model_view")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)
