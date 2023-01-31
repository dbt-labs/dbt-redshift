from pathlib import Path

import pytest
from dbt.tests.util import run_dbt


_MODELS = {
    "backup_is_false.sql": "{{ config(materialized='table', backup=False) }} select 1",
    "backup_is_true.sql": "{{ config(materialized='table', backup=True) }} select 1",
    "backup_is_undefined.sql": "{{ config(materialized='table') }} select 1",
    "backup_is_true_view.sql": "{{ config(materialized='view', backup=True) }} select 1",
    "syntax_with_distkey.sql": "{{ config(materialized='table', backup=False, dist='distkey') }} select 1 as distkey",
    "syntax_with_sortkey.sql": "{{ config(materialized='table', backup=False, sort='sortkey') }} select 1 as sortkey",
}


class BackupTableBase:

    @pytest.fixture(scope="class")
    def models(self):
        return _MODELS

    @staticmethod
    def run_dbt_once(project):
        project_root = Path(project.project_root)
        run_root = project_root / "target" / "run"
        if not run_root.exists():
            run_dbt(["run"])

    @staticmethod
    def get_ddl(model_name: str, project) -> str:
        with open(f"{project.project_root}/target/run/test/models/{model_name}.sql", 'r') as ddl_file:
            ddl_statement = ' '.join(ddl_file.readlines())
            return ddl_statement.lower()


class TestBackupTableSetup(BackupTableBase):

    def test_setup_executed_correctly(self, project):
        processed_models = run_dbt(["run"]).results
        assert len(processed_models) == len(_MODELS)


class TestBackupTableModel(BackupTableBase):

    @pytest.mark.parametrize("model_name,expected", [
        ("backup_is_false", False),
        ("backup_is_true", True),
        ("backup_is_undefined", True),
        ("backup_is_true_view", True),
    ])
    def test_setting_reflects_config_option(self, model_name: str, expected: bool, project):
        self.run_dbt_once(project)
        assert ("backup no" not in self.get_ddl(model_name, project)) == expected

    def test_properly_formed_ddl_distkey(self, project):
        self.run_dbt_once(project)
        ddl = self.get_ddl("syntax_with_distkey", project)
        assert ddl.find("backup no") < ddl.find("diststyle key distkey")

    def test_properly_formed_ddl_sortkey(self, project):
        self.run_dbt_once(project)
        ddl = self.get_ddl("syntax_with_sortkey", project)
        assert ddl.find("backup no") < ddl.find("compound sortkey")


class TestBackupTableProject(BackupTableBase):

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"backup": False}}

    @pytest.mark.parametrize("model_name,expected", [
        ("backup_is_true", True),
        ("backup_is_undefined", False)
    ])
    def test_setting_defaults_to_project_option(self, model_name: str, expected: bool, project):
        self.run_dbt_once(project)
        assert ("backup no" not in self.get_ddl(model_name, project)) == expected
