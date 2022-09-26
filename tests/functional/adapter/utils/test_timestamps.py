import pytest
from dbt.tests.adapter.utils.test_timestamps import TestCurrentTimestamps


class TestCurrentTimestampSnowflake(TestCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "timestamp without time zone", 
            "current_timestamp_in_utc": "timestamp without time zone"
        }    