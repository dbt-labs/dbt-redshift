import pytest
from dbt.tests.adapter.simple_seed.test_seed_type_override import BaseSimpleSeedColumnOverride
from dbt.tests.adapter.utils.base_utils import run_dbt

_SCHEMA_YML = """
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


class TestSimpleSeedColumnOverride(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def schema(self):
        return "simple_seed"

    @pytest.fixture(scope="class")
    def models(self):
        return {"models-rs.yml": _SCHEMA_YML}

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "text",
            "birthday": "date",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            "seed_id_str": "text",
            "looks_like_a_bool": "text",
            "looks_like_a_date": "text",
        }

    def test_redshift_simple_seed_with_column_override_redshift(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 2
        test_results = run_dbt(["test"])
        assert len(test_results) == 10
