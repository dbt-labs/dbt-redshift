from dbt.tests.adapter.grants.base_grants import BaseGrants
import pytest
import os

TEST_USER_ENV_VARS = ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"]
TEST_GROUP_ENV_VARS = ["DBT_TEST_GROUP_1", "DBT_TEST_GROUP_2", "DBT_TEST_GROUP_3"]
TEST_ROLE_ENV_VARS = ["DBT_TEST_ROLE_1", "DBT_TEST_ROLE_2", "DBT_TEST_ROLE_3"]


def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text


def get_test_permissions(permission_env_vars):
    test_permissions = []
    for env_var in permission_env_vars:
        permission_name = os.getenv(env_var)
        if permission_name:
            test_permissions.append(permission_name)
    return test_permissions


class BaseGrantsRedshift(BaseGrants):
    @pytest.fixture(scope="class", autouse=True)
    def get_test_groups(self, project):
        return get_test_permissions(TEST_GROUP_ENV_VARS)

    @pytest.fixture(scope="class", autouse=True)
    def get_test_roles(self, project):
        return get_test_permissions(TEST_ROLE_ENV_VARS)

    # This is an override of the BaseGrants class
    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        adapter = project.adapter
        # need a case-insensitive comparison
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = adapter.diff_of_two_nested_dicts(actual_grants, expected_grants)
        diff_b = adapter.diff_of_two_nested_dicts(expected_grants, actual_grants)
        assert diff_a == diff_b == {}
