import time

import pytest
import threading

from dbt.tests.util import run_dbt
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from tests.functional.adapter.concurrent_transaction.fixtures import *


class BaseConcurrentTransaction:
    @pytest.fixture(scope="function", autouse=True)
    def setUp(self, project):
        # Resetting the query_state
        self.query_state = {
            'view_model': 'wait',
            'model_1': 'wait',
        }

    @pytest.fixture(scope="class")
    def schema(self):
        return "concurrent_transaction"

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "udfs.sql": create_udfs_sql
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        print("Running")
        return {
            "on-run-start": [
                "{{ create_udfs() }}",
            ],
        }

    def run_select_and_check(self, rel, sql, project):
        connection_name = f"__test_{id(threading.current_thread())}"
        try:
            with project.adapter.connection_named(connection_name):
                res = project.run_sql(sql=sql, fetch='one')

                # The result is the output of f_sleep(), which is True
                if res[0]:
                    self.query_state[rel] = 'good'
                else:
                    self.query_state[rel] = 'bad'

        except TypeError as te:
            # Throws argument missing errors
            self.query_state[rel] = 'error: {}'.format(te)

    def async_select(self, rel, project, sleep=10):
        # Run the select statement in a thread. When the query returns, the global
        # query_state will be updated with a state of good/bad/error, and the associated
        # error will be reported if one was raised.

        schema = f"{project.test_schema}"
        query = f"""
                -- async_select: {rel}
                select {schema}.f_sleep({sleep}) from {schema}.{rel}
                """

        thread = threading.Thread(target=self.run_select_and_check,
                                  args=(rel, query, project))
        thread.start()

        return thread

    def test_concurrent_transaction(self, project):
        # First run the project to make sure the models exist
        results = run_dbt(['run'])
        assert len(results) == 2

        # Execute long-running queries in threads
        t1 = self.async_select('view_model', project, 10)
        t2 = self.async_select('model_1', project, 5)

        # While the queries are executing, re-run the project
        threads_result = run_dbt(['run', '--threads', '8'])
        assert len(threads_result) == 2

        # Finally, wait for these threads to finish
        t1.join()
        t2.join()

        assert len(threads_result) > 0

        # If the query succeeded, the global query_state should be 'good'
        assert self.query_state['view_model'] == 'good'
        assert self.query_state['model_1'] == 'good'


class TestTableConcurrentTransaction(BaseConcurrentTransaction):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": table_model_sql,
            "view_model.sql": view_sql
        }


class TestViewConcurrentTransaction(BaseConcurrentTransaction):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": view_model_sql,
            "view_model.sql": view_sql
        }


class TestIncrementalConcurrentTransaction(BaseConcurrentTransaction):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": incremental_model_sql,
            "view_model.sql": view_sql
        }
