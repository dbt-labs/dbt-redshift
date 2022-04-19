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
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats, expected_references_catalog
from tests.functional.adapter.expected_stats import redshift_stats

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



class TestDocsGenerateRedshift(BaseDocsGenerate):
    @pytest.fixture(scope="class")                               
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,                               
            role=profile_user,                    
            id_type="integer",                      
            text_type=AnyStringWith("character varying"),
            time_type="timestamp without time zone",
            view_type="VIEW",                
            table_type="BASE TABLE",                 
            model_stats=no_stats(),
            seed_stats=redshift_stats(),
        )             


class TestDocsGenReferencesRedshift(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return expected_references_catalog(
            project,
            role=profile_user,
            id_type="integer",
            text_type=AnyStringWith("character varying"),
            time_type="timestamp without time zone",
            bigint_type="bigint",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=redshift_stats(),
            seed_stats=redshift_stats(),
            view_summary_stats=no_stats(),
        )

