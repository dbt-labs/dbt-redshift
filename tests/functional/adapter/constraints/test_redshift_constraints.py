import pytest
import re
import json
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture
)
from dbt.tests.adapter.constraints.test_constraints import (
  BaseConstraintsColumnsEqual,
  BaseConstraintsRuntimeEnforcement
)

_expected_sql_redshift = """
  create  table
    "{0}"."{1}"."my_model__dbt_tmp"
    

  (
      id integer not null,
      color text,
      date_day date,
      primary key(id)
  )

    
  
    
    
    
  ;

  insert into "{0}"."{1}"."my_model__dbt_tmp"
    (
      

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
    )
  ;
"""

class TestRedshiftConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    pass


class TestRedshiftConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        return _expected_sql_redshift.format(project.database, project.test_schema)
    
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['Cannot insert a NULL value into column id']
    pass
