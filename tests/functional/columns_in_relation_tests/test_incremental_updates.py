from dbt.tests.util import run_dbt
import pytest

from tests.functional.utils import update_model


SEED = """
id,col7,col6,occurred_at
1,a,green,'2024-01-01'
2,b,green,'2024-01-01'
3,c,green,'2024-01-01'
""".strip()


SEED_UPDATES = """
id,col7,col6,occurred_at
4,b,red,'2024-02-01'
5,c,red,'2024-02-01'
6,c,blue,'2024-03-01'
""".strip()


MODEL = """
{{ config(materialized='incremental') }}
select * from {{ ref('my_seed') }}
where occurred_at::timestamptz >= '2024-01-01'::timestamptz
and occurred_at::timestamptz < '2024-02-01'::timestamptz
"""


MODEL_UPDATES = """
{{ config(materialized='incremental') }}
select * from {{ ref('my_seed') }}
where occurred_at::timestamptz >= '2024-02-01'::timestamptz
and occurred_at::timestamptz < '2024-03-01'::timestamptz
"""


class TestIncrementalUpdates:
    """
    This addresses: https://github.com/dbt-labs/dbt-redshift/issues/914

    We test it with the `restrict_direct_pg_catalog_access` flag both off and on since the bug
    only emerges when the flag is on (the former is a control).
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"restrict_direct_pg_catalog_access": False}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": SEED, "my_seed_updates.csv": SEED_UPDATES}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": MODEL}

    def test_columns_in_relation(self, project):
        # create the initial table
        run_dbt(["seed"])
        run_dbt(["run"])

        # verify the table starts with the initial records
        sql = (
            f"select count(*) as row_count from {project.database}.{project.test_schema}.my_model"
        )
        assert project.run_sql(sql, fetch="one")[0] == 3

        # move forward in time and pick up records in the source that should generate an incremental
        sql = f"""
        insert into {project.database}.{project.test_schema}.my_seed
        select * from {project.database}.{project.test_schema}.my_seed_updates
        """
        project.run_sql(sql)
        update_model(project, "my_model", MODEL_UPDATES)

        # apply the incremental
        run_dbt(["run"])

        # verify the new records made it into the table
        sql = (
            f"select count(*) as row_count from {project.database}.{project.test_schema}.my_model"
        )
        assert project.run_sql(sql, fetch="one")[0] == 5


class TestIncrementalUpdatesFlagOn(TestIncrementalUpdates):

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"restrict_direct_pg_catalog_access": True}}
