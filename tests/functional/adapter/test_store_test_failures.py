from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
)
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    TestStoreTestFailures,
)


class TestRedshiftTestStoreTestFailures(TestStoreTestFailures):
    pass


class TestRedshiftStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions):
    pass


class TestRedshiftStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff):
    pass


class TestRedshiftStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView):
    pass


class TestRedshiftStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric):
    pass
