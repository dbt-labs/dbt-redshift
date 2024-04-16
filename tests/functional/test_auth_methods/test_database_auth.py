import os

import pytest

from dbt.adapters.redshift.connections import RedshiftConnectionMethod
from dbt.tests.util import run_dbt

from tests.functional.test_auth_methods import files


class TestDatabaseAuth:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "method": RedshiftConnectionMethod.DATABASE.value,
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": int(os.getenv("REDSHIFT_TEST_PORT")),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "pass": os.getenv("REDSHIFT_TEST_PASS"),
            "threads": 1,
            "retries": 6,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        yield {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_view.sql": files.MY_VIEW}

    def test_connection(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
