import pytest
import os

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "redshift",
        "threads": 1,
        "retries": 6,
        "host": os.getenv("REDSHIFT_TEST_HOST"),
        "port": int(os.getenv("REDSHIFT_TEST_PORT")),
        "user": os.getenv("REDSHIFT_TEST_USER"),
        "pass": os.getenv("REDSHIFT_TEST_PASS"),
        "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
    }


def pytest_sessionfinish(session, exitstatus):
    """
    Configures pytest to treat a scenario with no tests as passing

    pytest returns a code 5 when it collects no tests in an effort to warn when tests are expected but not collected
    We don't want this when running tox because some combinations of markers and test segments return nothing
    """
    if exitstatus == 5:
        session.exitstatus = 0
