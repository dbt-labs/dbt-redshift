import pytest
import os

from dbt.tests.util import (
    relation_from_name,
    get_connection,
)

TEST_USER_ENV_VARS = ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"]
TEST_GROUP_ENV_VARS = ["DBT_TEST_GROUP_1", "DBT_TEST_GROUP_2", "DBT_TEST_GROUP_3"]
TEST_ROLE_ENV_VARS = ["DBT_TEST_ROLE_1", "DBT_TEST_ROLE_2", "DBT_TEST_ROLE_3"]


def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text


class BaseGrantsRedshift:
    def privilege_grantee_name_overrides(self):
        # these privilege and grantee names are valid on most databases, but not all!
        # looking at you, BigQuery
        # optionally use this to map from "select" --> "other_select_name", "insert" --> ...
        return {
            "select": "select",
            "insert": "insert",
            "fake_privilege": "fake_privilege",
            "invalid_user": "invalid_user",
        }

    def interpolate_name_overrides(self, yaml_text):
        return replace_all(yaml_text, self.privilege_grantee_name_overrides())

    @pytest.fixture(scope="class", autouse=True)
    def get_test_groups(self, project):
        test_groups = []
        for env_var in TEST_GROUP_ENV_VARS:
            group_name = os.getenv(env_var)
            if group_name:
                test_groups.append(group_name)
        return test_groups

    @pytest.fixture(scope="class", autouse=True)
    def get_test_roles(self, project):
        test_roles = []
        for env_var in TEST_ROLE_ENV_VARS:
            role_name = os.getenv(env_var)
            if role_name:
                test_roles.append(role_name)
        return test_roles

    @pytest.fixture(scope="class", autouse=True)
    def get_test_users(self, project):
        test_users = []
        for env_var in TEST_USER_ENV_VARS:
            user_name = os.getenv(env_var)
            if user_name:
                test_users.append(user_name)
        return test_users

    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            _, grant_table = adapter.execute(show_grant_sql, fetch=True)
            actual_grants = adapter.standardize_grants_dict(grant_table)
        return actual_grants

    # This is an override of the BaseGrants class
    def assert_expected_grants_match_actual(self, project, actual_grants, expected_grants):
        adapter = project.adapter
        # need a case-insensitive comparison
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = adapter.diff_of_two_nested_dicts(actual_grants, expected_grants)
        diff_b = adapter.diff_of_two_nested_dicts(expected_grants, actual_grants)
        assert diff_a == diff_b == {}
