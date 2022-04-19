import pytest

from dbt.tests.util import AnyStringWith, AnyFloat

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats

from dbt.tests.adapter.basic.files import seeds_base_csv, seeds_added_csv, seeds_newcolumns_csv

# set the datatype of the name column in the 'added' seed so it
# can hold the '_update' that's added
schema_seed_added_yml = """
version: 2
seeds:
  - name: added
    config:
      column_types:
        name: varchar(64)
"""


class TestSimpleMaterializationsRedshift(BaseSimpleMaterializations):
    pass


class TestSingularTestsRedshift(BaseSingularTests):
    pass


class TestSingularTestsEphemeralRedshift(BaseSingularTestsEphemeral):
    pass


class TestEmptyRedshift(BaseEmpty):
    pass


class TestEphemeralRedshift(BaseEphemeral):
    pass


class TestIncrementalRedshift(BaseIncremental):
    pass


class TestGenericTestsRedshift(BaseGenericTests):
    pass


class TestSnapshotCheckColsRedshift(BaseSnapshotCheckCols):
    # Redshift defines the 'name' column such that it's not big enough
    # to hold the '_update' added in the test.
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "seeds.yml": schema_seed_added_yml,
        }


class TestSnapshotTimestampRedshift(BaseSnapshotTimestamp):
    # Redshift defines the 'name' column such that it's not big enough
    # to hold the '_update' added in the test.
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "newcolumns.csv": seeds_newcolumns_csv,
            "seeds.yml": schema_seed_added_yml,
        }

class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


def redshift_stats():
    return {
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": True,
            "description": "Indicates whether there are statistics for this table",
            "include": False
        },
        "encoded": {
            "id": "encoded",
            "label": "Encoded",
            "value": AnyStringWith('Y'),
            "description": "Indicates whether any column in the table has compression encoding defined.",
            "include": True
        },
        "diststyle": {
            "id": "diststyle",
            "label": "Dist Style",
            "value": AnyStringWith('AUTO'),
            "description": "Distribution style or distribution key column, if key distribution is defined.",
            "include": True
        },
        "max_varchar": {
            "id": "max_varchar",
            "label": "Max Varchar",
            "value": AnyFloat(),
            "description": "Size of the largest column that uses a VARCHAR data type.",
            "include": True
        },
        "size": {
            "id": "size",
            "label": "Approximate Size",
            "value": AnyFloat(),
            "description": "Approximate size of the table, calculated from a count of 1MB blocks",
            "include": True
        },
        'sortkey1': {
            'id': 'sortkey1',
            'label': 'Sort Key 1',
            'value': AnyStringWith('AUTO'),
            'description': 'First column in the sort key.',
            'include': True,
        },
        "pct_used": {
            "id": "pct_used",
            "label": "Disk Utilization",
            "value": AnyFloat(),
            "description": "Percent of available space that is used by the table.",
            "include": True
        },
        "stats_off": {
            "id": "stats_off",
            "label": "Stats Off",
            "value": AnyFloat(),
            "description": "Number that indicates how stale the table statistics are; 0 is current, 100 is out of date.",
            "include": True
        },
        "rows": {
            "id": "rows",
            "label": "Approximate Row Count",
            "value": AnyFloat(),
            "description": "Approximate number of rows in the table. This value includes rows marked for deletion, but not yet vacuumed.",
            "include": True
        },
    }

class TestDocsGenerateRedshift(BaseDocsGenerate):
    @pytest.fixture(scope="class")                               
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,                               
            role=profile_user,                    
            id_type="integer",                      
            text_type=AnyStringWith('character varying'),
            time_type="timestamp without time zone",
            view_type="VIEW",                
            table_type="BASE TABLE",                 
            model_stats=no_stats(),
            seed_stats=redshift_stats(),
        )             

