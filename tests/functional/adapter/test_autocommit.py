import os
import pytest

from dbt.tests.util import run_dbt_and_capture

_MACROS__CREATE_DB = """
{% macro create_db_fake() %}

{% set database = "db_for_test__do_delete_if_you_see_this" %}

{# IF NOT EXISTS not avaiable but Redshift merely returns an error for trying to overwrite #}
{% set create_command %}
    CREATE DATABASE {{ database }}
{% endset %}

{{ log(create_command, info=True) }}

{% do run_query(create_command) %}

{{ log("Created redshift database " ~ database, info=True) }}

{% endmacro %}
"""


class TestAutocommitWorksWithTransactionBlocks:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACROS__CREATE_DB}

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
            "user": os.getenv("POSTGRES_TEST_USER", "root"),
            "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
            "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
            "autocommit": True,
        }

    def test_autocommit_allows_for_more_commands(self, project):
        """Scenario: user has autocommit=True in their target to run macros with normally
        forbidden commands like CREATE DATABASE and VACUUM"""
        result, out = run_dbt_and_capture(["run-operation", "create_db_fake"], expect_pass=False)
        assert "CREATE DATABASE cannot run inside a transaction block" not in out


class TestTransactionBlocksPreventCertainCommands:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macro.sql": _MACROS__CREATE_DB}

    def test_normally_create_db_disallowed(self, project):
        """Monitor if status quo in Cedshift connector changes"""
        result, out = run_dbt_and_capture(["run-operation", "create_db_fake"], expect_pass=False)
        assert "CREATE DATABASE cannot run inside a transaction block" in out
