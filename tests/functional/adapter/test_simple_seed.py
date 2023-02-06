import pytest
from dbt.tests.adapter.simple_seed.fixtures import macros__schema_test
from dbt.tests.adapter.simple_seed.seeds import (
    seeds__enabled_in_config_csv,
    seeds__tricky_csv
)
from dbt.tests.adapter.utils.base_utils import run_dbt

_PROPERTIES_SCHEMA_YML = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    tests:
    - column_type:
        type: date
  - name: seed_id
    tests:
    - column_type:
        type: character varying(256)

- name: seed_tricky
  columns:
  - name: seed_id
    tests:
    - column_type:
        type: integer
  - name: seed_id_str
    tests:
    - column_type:
        type: character varying(256)
  - name: a_bool
    tests:
    - column_type:
        type: boolean
  - name: looks_like_a_bool
    tests:
    - column_type:
        type: character varying(256)
  - name: a_date
    tests:
    - column_type:
        type: timestamp without time zone
  - name: looks_like_a_date
    tests:
    - column_type:
        type: character varying(256)
  - name: relative
    tests:
    - column_type:
        type: character varying(9)
  - name: weekday
    tests:
    - column_type:
        type: character varying(8)
""".lstrip()


class TestSimpleSeedColumnOverride:
    @pytest.fixture(scope="class")
    def schema(self):
        return "simple_seed"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_enabled.csv": seeds__enabled_in_config_csv,
            "seed_tricky.csv": seeds__tricky_csv
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "schema_test.sql": macros__schema_test,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "models-rs.yml": _PROPERTIES_SCHEMA_YML
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'seeds': {
                'test': {
                    'enabled': False,
                    'quote_columns': True,
                    'seed_enabled': {
                        'enabled': True,
                        '+column_types': self.seed_enabled_types()
                    },
                    'seed_tricky': {
                        'enabled': True,
                        '+column_types': self.seed_tricky_types(),
                    },
                },
            },
        }

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "text",
            "birthday": "date",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            'seed_id_str': 'text',
            'looks_like_a_bool': 'text',
            'looks_like_a_date': 'text',
        }

    def test_redshift_simple_seed_with_column_override_redshift(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 2
        test_results = run_dbt(["test"])
        assert len(test_results) == 10
