from dbt.tests.adapter.constraints.test_constraints import (
    TestModelLevelConstraintsEnabledConfigs,
    TestModelLevelConstraintsDisabledConfigs,
    TestSchemaConstraintsEnabledConfigs,
    TestModelLevelConstraintsErrorMessages
)


class TestRedshiftModelLevelConstraintsEnabledConfigs(TestModelLevelConstraintsEnabledConfigs):
    pass

class TestRedshiftModelLevelConstraintsDisabledConfigs(TestModelLevelConstraintsDisabledConfigs):
    pass

class TestRedshiftSchemaConstraintsEnabledConfigs(TestSchemaConstraintsEnabledConfigs):
    pass

class TestRedshiftModelLevelConstraintsErrorMessages(TestModelLevelConstraintsErrorMessages):
    pass
