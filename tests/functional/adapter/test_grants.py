import pytest
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants, snapshot_schema_yml


my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select 1 as id, cast('blue' as {{ type_string() }}) as color
{% endsnapshot %}
""".strip()


class TestModelGrantsRedshift(BaseModelGrants):
    pass


class TestIncrementalGrantsRedshift(BaseIncrementalGrants):
    pass


class TestSeedGrantsRedshift(BaseSeedGrants):
    pass


class TestSnapshotGrantsRedshift(BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"my_snapshot.sql": my_snapshot_sql, "schema.yml": snapshot_schema_yml}


class TestInvalidGrantsRedshift(BaseModelGrants):
    pass
