import pytest
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    write_file,
)
from tests.functional.adapter.grants.base_grants import BaseGrantsRedshift

my_model_sql = """
  select 1 as fun
"""

table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

multiple_users_table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}", "{{ env_var('DBT_TEST_USER_2') }}"]
"""

multiple_privileges_table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
        insert: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

# table materialization single select
extended_table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_1') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_1') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_1') }}"]
"""

# table materialization change select
extended2_table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_2') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_2') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_2') }}"]
"""

# table materialization multiple grantees
extended_multiple_grantees_table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_1') }}", "{{ env_var('DBT_TEST_USER_2') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_1') }}", "{{ env_var('DBT_TEST_GROUP_2') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_1') }}", "{{ env_var('DBT_TEST_ROLE_2') }}"]
"""
# table materialization multiple privileges
extended_multiple_privileges_table_model_schema_yml = """
version: 2
models:
  - name: my_model_table
    config:
      materialized: table
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_1') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_1') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_1') }}"]
        insert:
          user: ["{{ env_var('DBT_TEST_USER_2') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_2') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_2') }}"]
"""


class TestModelGrantsTableRedshift(BaseGrantsRedshift):
    @pytest.fixture(scope="class")
    def models(self):
        updated_schema = self.interpolate_name_overrides(table_model_schema_yml)
        return {
            "my_model_table.sql": my_model_sql,
            "schema.yml": updated_schema,
        }

    def test_table_grants(self, project, get_test_users, get_test_groups, get_test_roles):
        # Override/refactor the tests from dbt-core #
        # we want the test to fail, not silently skip
        test_users = get_test_users
        test_groups = get_test_groups
        test_roles = get_test_roles
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        insert_privilege_name = self.privilege_grantee_name_overrides()["insert"]
        assert len(test_users) == 3
        assert len(test_groups) == 3
        assert len(test_roles) == 3

        # Table materialization, single select grant
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_table"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: {"user": [test_users[0]]}}
        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Table materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: {"user": [test_users[1]]}}
        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Table materialization, multiple grantees
        updated_yaml = self.interpolate_name_overrides(multiple_users_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {select_privilege_name: {"user": [test_users[0], test_users[1]]}}
        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Table materialization, multiple privileges
        updated_yaml = self.interpolate_name_overrides(multiple_privileges_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        user_expected = {
            select_privilege_name: [test_users[0]],
            insert_privilege_name: [test_users[1]],
        }
        assert model.config.grants == user_expected
        assert model.config.materialized == "table"
        expected = {
            select_privilege_name: {"user": [test_users[0]]},
            insert_privilege_name: {"user": [test_users[1]]},
        }
        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Additional tests for privilege grants to extended permission types
        # Table materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(extended_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {
            select_privilege_name: {
                "user": [test_users[0]],
                "group": [test_groups[0]],
                "role": [test_roles[0]],
            }
        }

        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Table materialization, change select grant
        updated_yaml = self.interpolate_name_overrides(extended2_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {
            select_privilege_name: {
                "user": [test_users[1]],
                "group": [test_groups[1]],
                "role": [test_roles[1]],
            }
        }
        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Table materialization, multiple grantees
        updated_yaml = self.interpolate_name_overrides(
            extended_multiple_grantees_table_model_schema_yml
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {
            select_privilege_name: {
                "user": [test_users[0], test_users[1]],
                "group": [test_groups[0], test_groups[1]],
                "role": [test_roles[0], test_roles[1]],
            }
        }
        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # Table materialization, multiple privileges
        updated_yaml = self.interpolate_name_overrides(
            extended_multiple_privileges_table_model_schema_yml
        )
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        results = run_dbt(["run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "table"
        expected = {
            select_privilege_name: {
                "user": [test_users[0]],
                "group": [test_groups[0]],
                "role": [test_roles[0]],
            },
            insert_privilege_name: {
                "user": [test_users[1]],
                "group": [test_groups[1]],
                "role": [test_roles[1]],
            },
        }
        actual_grants = self.get_grants_on_relation(project, "my_model_table")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)
