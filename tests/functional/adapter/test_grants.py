from grants.test_model_grants import BaseModelGrantsRedshift
from grants.test_snapshot_grants import BaseSnapshotGrantsRedshift
from grants.test_seed_grants import BaseSeedGrantsRedshift
from grants.test_incremental_grants import BaseIncrementalGrantsRedshift


class TestModelGrantsRedshift(BaseModelGrantsRedshift):
    pass


class TestIncrementalGrantsRedshift(BaseIncrementalGrantsRedshift):
    pass


class TestSeedGrantsRedshift(BaseSeedGrantsRedshift):
    pass


class TestSnapshotGrantsRedshift(BaseSnapshotGrantsRedshift):
    pass


class TestInvalidGrantsRedshift(BaseModelGrantsRedshift):
    pass