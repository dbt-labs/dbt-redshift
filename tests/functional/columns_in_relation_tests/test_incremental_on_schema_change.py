from dbt.tests.util import run_dbt
import pytest

from tests.functional.utils import update_model


SEED = """
column_a,column_b,column_c,column_d
1,thunder,ho,Cheetara
2,THUNDER,HO,Tygra
3,THUNDERCATS,HOOOO,Lion-O
""".strip()


MODEL_INITIAL = """
{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
) }}
select
    column_a,
    column_b,
    column_c
from {{ ref('my_seed') }}
"""


MODEL_UPDATE = """
{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
) }}
select
    column_b as column_B,
    column_c as "COLUMN_C",
    column_D
from {{ ref('my_seed') }}
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
