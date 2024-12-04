import pytest
from dbt.tests.util import (
    run_dbt,
    run_dbt_and_capture,
    get_manifest,
    write_file,
)

from tests.functional.adapter.grants.base_grants import BaseGrantsRedshift


my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select 1 as id, cast('blue' as {{ type_string() }}) as color
{% endsnapshot %}
""".strip()

snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""

user2_snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

extended_snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_1') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_1') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_1') }}"]
"""

extended2_snapshot_schema_yml = """
version: 2
snapshots:
  - name: my_snapshot
    config:
      grants:
        select:
          user: ["{{ env_var('DBT_TEST_USER_2') }}"]
          group: ["{{ env_var('DBT_TEST_GROUP_2') }}"]
          role: ["{{ env_var('DBT_TEST_ROLE_2') }}"]
"""


class TestSnapshotGrantsRedshift(BaseGrantsRedshift):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": self.interpolate_name_overrides(snapshot_schema_yml),
        }

    def test_snapshot_grants(self, project, get_test_users, get_test_groups, get_test_roles):
        print("snapshot testing")
        test_users = get_test_users
        test_groups = get_test_groups
        test_roles = get_test_roles
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]

        # run the snapshot
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        snapshot_id = "snapshot.test.my_snapshot"
        snapshot = manifest.nodes[snapshot_id]
        user_expected = {select_privilege_name: [test_users[0]]}
        assert snapshot.config.grants == user_expected
        expected = {select_privilege_name: {"user": [test_users[0]]}}
        actual_grants = self.get_grants_on_relation(project, "my_snapshot")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # run it again, nothing should have changed
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output
        actual_grants = self.get_grants_on_relation(project, "my_snapshot")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # change the grantee, assert it updates
        updated_yaml = self.interpolate_name_overrides(user2_snapshot_schema_yml)
        write_file(updated_yaml, project.project_root, "snapshots", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        expected = {select_privilege_name: {"user": [test_users[1]]}}
        actual_grants = self.get_grants_on_relation(project, "my_snapshot")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # change the grants, assert that it updates
        updated_yaml = self.interpolate_name_overrides(extended_snapshot_schema_yml)
        write_file(updated_yaml, project.project_root, "snapshots", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        expected = {
            select_privilege_name: {
                "user": [test_users[0]],
                "group": [test_groups[0]],
                "role": [test_roles[0]],
            }
        }
        actual_grants = self.get_grants_on_relation(project, "my_snapshot")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)

        # change the grants again, assert that it updates
        updated_yaml = self.interpolate_name_overrides(extended2_snapshot_schema_yml)
        write_file(updated_yaml, project.project_root, "snapshots", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "snapshot"])
        assert len(results) == 1
        expected = {
            select_privilege_name: {
                "user": [test_users[1]],
                "group": [test_groups[1]],
                "role": [test_roles[1]],
            }
        }
        actual_grants = self.get_grants_on_relation(project, "my_snapshot")
        self.assert_expected_grants_match_actual(project, actual_grants, expected)
