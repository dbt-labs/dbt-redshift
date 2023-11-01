import pytest
from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    write_file,
    relation_from_name,
    get_connection,
)

from tests.functional.adapter.grants.base_grants import BaseGrantsRedshift

my_incremental_model_sql = """
  select 1 as fun
"""

incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

extended_incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_1') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_1') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_1') }}"]
"""

extended2_incremental_model_schema_yml = """
version: 2
models:
  - name: my_incremental_model
    config:
      materialized: incremental
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_2') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_2') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_2') }}"]
"""


class TestIncrementalGrantsRedshift(BaseGrantsRedshift):
    @pytest.fixture(scope="class")
    def models(self):
        updated_schema = self.interpolate_name_overrides(incremental_model_schema_yml)
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "schema.yml": updated_schema,
        }

    def test_incremental_grants(self, project, get_test_users, get_test_groups, get_test_roles):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        test_groups = get_test_groups
        test_roles = get_test_roles
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3
        assert len(test_groups) == 3
        assert len(test_roles) == 3

        # Incremental materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_incremental_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: {"user": [test_users[0]]}}
        actual_grants = self.get_grants_on_relation(project, "my_incremental_model")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output  # with space to disambiguate from 'show grants'
        actual_grants = self.get_grants_on_relation(project, "my_incremental_model")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Incremental materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_incremental_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: {"user": [test_users[1]]}}
        actual_grants = self.get_grants_on_relation(project, "my_incremental_model")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Incremental materialization, same config, now with --full-refresh
        run_dbt(["--debug", "run", "--full-refresh"])
        assert len(results) == 1
        # whether grants or revokes happened will vary by adapter
        actual_grants = self.get_grants_on_relation(project, "my_incremental_model")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Now drop the schema (with the table in it)
        adapter = project.adapter
        relation = relation_from_name(adapter, "my_incremental_model")
        with get_connection(adapter):
            adapter.drop_schema(relation)

        # Incremental materialization, same config, rebuild now that table is missing
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "grant " in log_output
        assert "revoke " not in log_output
        actual_grants = self.get_grants_on_relation(project, "my_incremental_model")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Additional tests for privilege grants to extended permission types
        # Incremental materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(extended_incremental_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        # select grant to *_1 users, groups, and roles, select revoke from DBT_TEST_USER_2
        assert "grant " in log_output
        assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {
            select_privilege_name: {
                "user": [test_users[0]],
                "group": [test_groups[0]],
                "role": [test_roles[0]],
            }
        }

        actual_grants = self.get_grants_on_relation(project, "my_incremental_model")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Incremental materialization, change select grant
        updated_yaml = self.interpolate_name_overrides(extended2_incremental_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        # select grant to *_2 users,groups, and roles, select revoke from *_1 users, groups, and roles
        assert "grant " in log_output
        assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {
            select_privilege_name: {
                "user": [test_users[1]],
                "group": [test_groups[1]],
                "role": [test_roles[1]],
            }
        }
        actual_grants = self.get_grants_on_relation(project, "my_incremental_model")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)
