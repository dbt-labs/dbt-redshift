import pytest
import os

from dbt.exceptions import DbtDatabaseError

# This is a hack to prevent the fixture from running more than once
GRANTS_AND_ROLES_SETUP = False


@pytest.fixture(scope="class", autouse=True)
def setup_grants_and_roles(project):
    global GRANTS_AND_ROLES_SETUP
    groups = [
        os.environ[env_var] for env_var in os.environ if env_var.startswith("DBT_TEST_GROUP_")
    ]
    roles = [os.environ[env_var] for env_var in os.environ if env_var.startswith("DBT_TEST_ROLE_")]
    if not GRANTS_AND_ROLES_SETUP:
        with project.adapter.connection_named("__test"):
            for group in groups:
                try:
                    project.adapter.execute(f"CREATE GROUP {group}")
                except DbtDatabaseError:
                    # This is expected if the group already exists
                    pass

            for role in roles:
                try:
                    project.adapter.execute(f"CREATE ROLE {role}")
                except DbtDatabaseError:
                    # This is expected if the group already exists
                    pass

            GRANTS_AND_ROLES_SETUP = True


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
