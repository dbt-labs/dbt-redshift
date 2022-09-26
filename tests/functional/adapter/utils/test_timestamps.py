import pytest
from dbt.tests.adapter.utils import test_timestamps


class TestCurrentTimestampSnowflake(test_timestamps.TestCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "timestamp without time zone", 
            "current_timestamp_in_utc": "timestamp without time zone"
        }    