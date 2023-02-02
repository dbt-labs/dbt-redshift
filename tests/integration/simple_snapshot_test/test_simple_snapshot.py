from tests.integration.base import DBTIntegrationTest


class BaseSimpleSnapshotTest(DBTIntegrationTest):
    NUM_SNAPSHOT_MODELS = 1

    @property
    def schema(self):
        return "simple_snapshot"

    @property
    def models(self):
        return "models"

    def run_snapshot(self):
        return self.run_dbt(['snapshot'])

    def dbt_run_seed_snapshot(self):
        self.run_sql_file('seed.sql')

        results = self.run_snapshot()
        self.assertEqual(len(results),  self.NUM_SNAPSHOT_MODELS)

    def assert_case_tables_equal(self, actual, expected):
        self.assertTablesEqual(actual, expected)

    def assert_expected(self):
        self.run_dbt(['test'])
        self.assert_case_tables_equal('snapshot_actual', 'snapshot_expected')


class TestCustomSnapshotFiles(BaseSimpleSnapshotTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'macro-paths': ['custom-snapshot-macros', 'macros'],
            'snapshot-paths': ['test-snapshots-pg-custom'],
        }


class TestNamespacedCustomSnapshotFiles(BaseSimpleSnapshotTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'macro-paths': ['custom-snapshot-macros', 'macros'],
            'snapshot-paths': ['test-snapshots-pg-custom-namespaced'],
        }


class TestInvalidNamespacedCustomSnapshotFiles(BaseSimpleSnapshotTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'macro-paths': ['custom-snapshot-macros', 'macros'],
            'snapshot-paths': ['test-snapshots-pg-custom-invalid'],
        }

    def run_snapshot(self):
        return self.run_dbt(['snapshot'], expect_pass=False)
