import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_data_tests import BaseDataTests
from dbt.tests.adapter.basic.test_data_tests_ephemeral import BaseDataTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_schema_tests import BaseSchemaTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp


class TestSimpleMaterializationsRedshift(BaseSimpleMaterializations):
    pass


class TestDataTestsRedshift(BaseDataTests):
    pass


class TestDataTestsEphemeralRedshift(BaseDataTestsEphemeral):
    pass


class TestEmptyRedshift(BaseEmpty):
    pass


class TestEphemeralRedshift(BaseEphemeral):
    pass


class TestIncrementalRedshift(BaseIncremental):
    pass


class TestSchemaTestsRedshift(BaseSchemaTests):
    pass


@pytest.mark.xfail(reason="Error: Value too long for character type")
class TestSnapshotCheckColsRedshift(BaseSnapshotCheckCols):
    pass


@pytest.mark.xfail(reason="Error: Value too long for character type")
class TestSnapshotTimestampRedshift(BaseSnapshotTimestamp):
    pass

