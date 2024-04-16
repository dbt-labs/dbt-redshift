import os

import pytest


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
