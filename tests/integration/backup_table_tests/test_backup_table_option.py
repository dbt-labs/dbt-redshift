import os

from tests.integration.base import DBTIntegrationTest, use_profile


class TestBackupTableOption(DBTIntegrationTest):
    @property
    def schema(self):
        return 'backup_table_tests'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("models")

    @property
    def project_config(self):
        return {
            'config-version': 2
        }

    def check_backup_param_template(self, test_table_name, backup_is_expected):
        # Use raw DDL statement to confirm backup is set correctly on new table
        with open('target/run/test/models/{}.sql'.format(test_table_name), 'r') as ddl_file:
            ddl_statement = ddl_file.readlines()
            lowercase_statement = ' '.join(ddl_statement).lower()
            self.assertEqual('backup no' not in lowercase_statement, backup_is_expected)

    @use_profile('redshift')
    def test__redshift_backup_table_option(self):
        self.assertEqual(len(self.run_dbt()), 6)

        # model_backup_undefined should not contain a BACKUP NO parameter in the table DDL
        self.check_backup_param_template('model_backup_undefined', True)

        # model_backup_true should not contain a BACKUP NO parameter in the table DDL
        self.check_backup_param_template('model_backup_true', True)

        # model_backup_false should contain a BACKUP NO parameter in the table DDL
        self.check_backup_param_template('model_backup_false', False)

        # Any view should not contain a BACKUP NO parameter, regardless of the specified config (create will fail)
        self.check_backup_param_template('model_backup_true_view', True)

class TestBackupTableOptionProjectFalse(DBTIntegrationTest):
    @property
    def schema(self):
        return 'backup_table_tests'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("models")

    @property
    def project_config(self):
        # Update project config to set backup to False.
        # This should make the 'model_backup_undefined' switch to BACKUP NO
        return {
            'config-version': 2,
            'models': {'backup': False}
        }

    def check_backup_param_template(self, test_table_name, backup_is_expected):
        # Use raw DDL statement to confirm backup is set correctly on new table
        with open('target/run/test/models/{}.sql'.format(test_table_name), 'r') as ddl_file:
            ddl_statement = ddl_file.readlines()
            lowercase_statement = ' '.join(ddl_statement).lower()
            self.assertEqual('backup no' not in lowercase_statement, backup_is_expected)

    @use_profile('redshift')
    def test__redshift_backup_table_option_project_config_false(self):
        self.assertEqual(len(self.run_dbt()), 6)

        # model_backup_undefined should contain a BACKUP NO parameter in the table DDL
        self.check_backup_param_template('model_backup_undefined', False)

        # model_backup_true should not contain a BACKUP NO parameter in the table DDL
        self.check_backup_param_template('model_backup_true', True)

        # model_backup_false should contain a BACKUP NO parameter in the table DDL
        self.check_backup_param_template('model_backup_false', False)

        # Any view should not contain a BACKUP NO parameter, regardless of the specified config (create will fail)
        self.check_backup_param_template('model_backup_true_view', True)


class TestBackupTableOptionOrder(DBTIntegrationTest):
    @property
    def schema(self):
        return 'backup_table_tests'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("models")

    @property
    def project_config(self):
        return {
            'config-version': 2
        }

    def check_backup_param_template(self, test_table_name, backup_flag_is_expected):
        # Use raw DDL statement to confirm backup is set correctly on new table
        with open('target/run/test/models/{}.sql'.format(test_table_name), 'r') as ddl_file:
            ddl_statement = ddl_file.readlines()
            lowercase_statement = ' '.join(ddl_statement).lower()
            self.assertEqual('backup no' not in lowercase_statement, backup_flag_is_expected)
            if backup_flag_is_expected:
                distkey_index = lowercase_statement.find('distkey')
                sortkey_index = lowercase_statement.find('sortkey')
                backup_index = lowercase_statement.find('backup no')
                self.assertEqual((backup_index < distkey_index) or distkey_index == -1, backup_flag_is_expected)
                self.assertEqual((backup_index < sortkey_index) or sortkey_index == -1, backup_flag_is_expected)

    @use_profile('redshift')
    def test__redshift_backup_table_option_project_config_false(self):
        self.assertEqual(len(self.run_dbt()), 6)

        # model_backup_param_before_distkey should contain a BACKUP NO parameter which precedes a DISTKEY in the table ddl
        self.check_backup_param_template('model_backup_param_before_distkey', False)
        
        # model_backup_param_before_sortkey should contain a BACKUP NO parameter which precedes a SORTKEY in the table ddl
        self.check_backup_param_template('model_backup_param_before_sortkey', False)
