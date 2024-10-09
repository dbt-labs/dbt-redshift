from dbt.tests.util import run_dbt
import pytest

from tests.functional.utils import update_model


SEED = """
id,col7,col6,occurred_at
1,Cheetara,thunder,'2024-01-01'
2,Tygra,thunder,'2024-01-01'
2,Tygra,THUNDER,'2024-02-01'
3,Lion-O,thunder,'2024-01-01'
3,Lion-O,THUNDER,'2024-02-01'
3,Lion-O,THUNDERCATS,'2024-03-01'
""".strip()


MODEL_INITIAL = """
{{ config(
    materialized='incremental',
    dist='col6',
    on_schema_change='append_new_columns',
) }}
select
    id::bigint as id,
    col6::varchar(128) as col6,
    occurred_at::timestamptz as occurred_at
from {{ ref('my_seed') }}
where occurred_at::timestamptz >= '2024-01-01'::timestamptz
and occurred_at::timestamptz < '2024-02-01'::timestamptz
"""


MODEL_UPDATE = """
{{ config(
    materialized='incremental',
    dist='col6',
    on_schema_change='append_new_columns',
) }}
select
    id::bigint as id,
    col6::varchar(128) as col6,
    occurred_at::timestamptz as occurred_at,
    col7::varchar(56) as col7
from {{ ref('my_seed') }}
where occurred_at::timestamptz >= '2024-02-01'::timestamptz
and occurred_at::timestamptz < '2024-03-01'::timestamptz
"""


class TestIncrementalOnSchemaChange:
    """
    This addresses: https://github.com/dbt-labs/dbt-redshift/issues/914
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"restrict_direct_pg_catalog_access": False}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": MODEL_INITIAL}

    def test_columns_in_relation(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        update_model(project, "my_model", MODEL_UPDATE)
        run_dbt(["run"])
        # a successful run is a pass
