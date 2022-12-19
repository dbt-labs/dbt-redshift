import pytest
import re
import json
from dbt.tests.util import (
    run_dbt,
)
from dbt.tests.adapter.constraints.test_constraints import (
    TestModelLevelConstraintsEnabledConfigs,
    TestModelLevelConstraintsDisabledConfigs,
    TestSchemaConstraintsEnabledConfigs,
    TestModelLevelConstraintsErrorMessages
)


my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      constraints_enabled: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
"""

schema_name = "schema_placeholder"

_expected_sql = f"""
  create  table
    "dbt"."{schema_name}"."my_model__dbt_tmp"
    

    (
        id integer not null,
        color varchar,
        date_day date,
        primary key(id)
    )
    
    
    
  ;

  insert into "dbt"."{schema_name}"."my_model__dbt_tmp"
    (
      

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
    )
  ;
"""


class BaseConstraintsEnabledModelvsProject:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+constraints_enabled": True,
                    "subdirectory": {
                        "+constraints_enabled": False,
                    },
                }
            }
        }


class TestRedshiftConstraints(BaseConstraintsEnabledModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql

    def test__model_constraints_DDL(self, project, expected_sql):
        results = run_dbt(["run"])
        assert len(results) == 1
        with open("./target/run/test/models/my_model.sql", "r") as fp:
            generated_sql = fp.read()

        with open("./target/manifest.json", "r") as fp:
            generated_manifest = json.load(fp)

        model_unique_id = 'model.test.my_model'
        schema_name_generated = (generated_manifest['nodes'][model_unique_id]['schema'])

        if expected_sql:
            expected_sql = expected_sql.replace(schema_name, schema_name_generated)
            generated_sql_check = re.sub(r"\s+", "", generated_sql).lower()
            expected_sql_check = re.sub(r"\s+", "", expected_sql).lower()
            assert (
                expected_sql_check == generated_sql_check
            ), f"generated sql did not match expected: {generated_sql}"

    def test__rollback(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        with open("./models/my_model.sql", "r") as fp:
            my_model_sql_original = fp.read()

        my_model_sql_error = my_model_sql_original.replace("1 as id", "null as id")

        with open("./models/my_model.sql", "w") as fp:
            fp.write(my_model_sql_error)

        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1

        with open("./target/manifest.json", "r") as fp:
            generated_manifest = json.load(fp)

        model_unique_id = 'model.test.my_model'
        schema_name_generated = (generated_manifest['nodes'][model_unique_id]['schema'])

        # verify the previous table exists
        sql = """
            select id from dbt.{0}.my_model where id = 1
        """.format(schema_name_generated)
        results = project.run_sql(sql, fetch="all")
        assert len(results) == 1
        assert results[0][0] == 1