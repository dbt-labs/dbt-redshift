import pytest
import os

from dbt_common.exceptions import DbtDatabaseError

# This is a hack to prevent the fixture from running more than once
GRANTS_AND_ROLES_SETUP = False

GROUPS = {
    "DBT_TEST_GROUP_1": "dbt_test_group_1",
    "DBT_TEST_GROUP_2": "dbt_test_group_2",
    "DBT_TEST_GROUP_3": "dbt_test_group_3",
}
ROLES = {
    "DBT_TEST_ROLE_1": "dbt_test_role_1",
    "DBT_TEST_ROLE_2": "dbt_test_role_2",
    "DBT_TEST_ROLE_3": "dbt_test_role_3",
}


@pytest.fixture(scope="class", autouse=True)
def setup_grants_and_roles(project):
    print("Start setup for groups and roles")

    global GRANTS_AND_ROLES_SETUP
    for env_name, env_var in GROUPS.items():
        os.environ[env_name] = env_var
    for env_name, env_var in ROLES.items():
        os.environ[env_name] = env_var
    # if not GRANTS_AND_ROLES_SETUP:
    if True:
        print("Create groups and roles")
        with project.adapter.connection_named("__test"):
            for group in GROUPS.values():
                try:
                    print(f"CREATE GROUP {group}")
                    project.adapter.execute(f"CREATE GROUP {group}")
                except DbtDatabaseError:
                    # This is expected if the group already exists
                    pass

            for role in ROLES.values():
                try:
                    print(f"CREATE ROLE {group}")
                    project.adapter.execute(f"CREATE ROLE {role}")
                except DbtDatabaseError:
                    # This is expected if the group already exists
                    pass

            GRANTS_AND_ROLES_SETUP = True

    print("End setup for groups and roles")


@pytest.fixture
def model_ddl(request) -> str:
    """
    Returns the contents of the DDL file for the model provided. Use with pytest parameterization.

    Example:
    ===
    @pytest.mark.parametrize(
        "model_ddl,backup_expected",
        [("backup_is_false", False)],
        indirect=["model_ddl"]
    )
    def test_setting_reflects_config_option(self, model_ddl: str, backup_expected: bool):
        backup_will_occur = "backup no" not in model_ddl.lower()
        assert backup_will_occur == backup_expected
    ===

    In this example, the fixture returns the contents of the backup_is_false DDL file as a string.
    This string is then referenced in the test as model_ddl.
    """
    with open(f"target/run/test/models/{request.param}.sql", "r") as ddl_file:
        yield "\n".join(ddl_file.readlines())
