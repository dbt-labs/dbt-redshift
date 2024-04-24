import os

import pytest

from dbt.adapters.redshift.connections import RedshiftConnectionMethod
from dbt.tests.util import run_dbt

from tests.functional.test_auth_methods import files


class IAMRoleAuth:
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


class TestIAMRoleAuthProfile(IAMRoleAuth):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "method": RedshiftConnectionMethod.IAM_ROLE.value,
            "iam_profile": os.getenv("REDSHIFT_TEST_IAM_ROLE_PROFILE"),
            "access_key_id": None,
            "secret_access_key": None,
            "iam_role_arn": None,
            "cluster_id": os.getenv("REDSHIFT_TEST_CLUSTER_ID"),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "role": None,
            "host": "",
            "port": 0,
            "threads": 1,
            "retries": 6,
        }
