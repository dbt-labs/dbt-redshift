import pytest
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)


seeds__expected_merge_exclude_columns_csv = """id,msg,color
1,hello,blue
2,goodbye,green
3,NULL,purple
"""


class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_merge_exclude_columns.csv": seeds__expected_merge_exclude_columns_csv}
