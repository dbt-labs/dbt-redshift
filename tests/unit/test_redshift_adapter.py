import unittest
from unittest import mock
from unittest.mock import Mock, call

import agate
import dbt
import redshift_connector

from dbt.adapters.redshift import (
    RedshiftAdapter,
    Plugin as RedshiftPlugin,
)
from dbt.clients import agate_helper
from dbt.exceptions import FailedToConnectError
from dbt.adapters.redshift.connections import RedshiftConnectMethodFactory, RedshiftSSLConfig
from .utils import (
    config_from_parts_or_dicts,
    mock_connection,
    TestAdapterConversions,
    inject_adapter,
)


DEFAULT_SSL_CONFIG = RedshiftSSLConfig().to_dict()


class TestRedshiftAdapter(unittest.TestCase):
    def setUp(self):
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "redshift",
                    "dbname": "redshift",
                    "user": "root",
                    "host": "thishostshouldnotexist.test.us-east-1",
                    "pass": "password",
                    "port": 5439,
                    "schema": "public",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = RedshiftAdapter(self.config)
            inject_adapter(self._adapter, RedshiftPlugin)
        return self._adapter

    @mock.patch("redshift_connector.connect", Mock())
    def test_implicit_database_conn(self):
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            timeout=None,
            region="us-east-1",
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", Mock())
    def test_explicit_database_conn(self):
        self.config.method = "database"

        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region="us-east-1",
            timeout=None,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", Mock())
    def test_explicit_iam_conn_without_profile(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            cluster_id="my_redshift",
            host="thishostshouldnotexist.test.us-east-1",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            db_user="root",
            password="",
            user="",
            cluster_identifier="my_redshift",
            region="us-east-1",
            timeout=None,
            auto_create=False,
            db_groups=[],
            profile=None,
            port=5439,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", Mock())
    def test_conn_timeout_30(self):
        self.config.credentials = self.config.credentials.replace(connect_timeout=30)
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region="us-east-1",
            timeout=30,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", Mock())
    @mock.patch("boto3.Session", Mock())
    def test_explicit_iam_conn_with_profile(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            cluster_id="my_redshift",
            iam_profile="test",
            host="thishostshouldnotexist.test.us-east-1",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle

        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            cluster_identifier="my_redshift",
            region="us-east-1",
            auto_create=False,
            db_groups=[],
            db_user="root",
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", Mock())
    @mock.patch("boto3.Session", Mock())
    def test_explicit_iam_serverless_with_profile(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region="us-east-2",
            auto_create=False,
            db_groups=[],
            db_user="root",
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", Mock())
    @mock.patch("boto3.Session", Mock())
    def test_explicit_region(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233.redshift-serverless.amazonaws.com",
            region="us-east-2",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region="us-east-2",
            auto_create=False,
            db_groups=[],
            db_user="root",
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", Mock())
    @mock.patch("boto3.Session", Mock())
    def test_explicit_region_failure(self):
        # Failure test with no region
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233_no_region",
            region=None,
        )

        with self.assertRaises(dbt.exceptions.FailedToConnectError):
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233_no_region",
                database="redshift",
                cluster_identifier=None,
                auto_create=False,
                db_groups=[],
                db_user="root",
                password="",
                user="",
                profile="test",
                timeout=None,
                port=5439,
                **DEFAULT_SSL_CONFIG,
            )

    @mock.patch("redshift_connector.connect", Mock())
    @mock.patch("boto3.Session", Mock())
    def test_explicit_invalid_region(self):
        # Invalid region test
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233_no_region.us-not-a-region-1",
            region=None,
        )

        with self.assertRaises(dbt.exceptions.FailedToConnectError):
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233_no_region",
                database="redshift",
                cluster_identifier=None,
                auto_create=False,
                db_groups=[],
                db_user="root",
                password="",
                user="",
                profile="test",
                timeout=None,
                port=5439,
                **DEFAULT_SSL_CONFIG,
            )

    @mock.patch("redshift_connector.connect", Mock())
    def test_sslmode_disable(self):
        self.config.credentials.sslmode = "disable"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region="us-east-1",
            timeout=None,
            ssl=False,
            sslmode=None,
        )

    @mock.patch("redshift_connector.connect", Mock())
    def test_sslmode_allow(self):
        self.config.credentials.sslmode = "allow"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region="us-east-1",
            timeout=None,
            ssl=True,
            sslmode="verify-ca",
        )

    @mock.patch("redshift_connector.connect", Mock())
    def test_sslmode_verify_full(self):
        self.config.credentials.sslmode = "verify-full"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region="us-east-1",
            timeout=None,
            ssl=True,
            sslmode="verify-full",
        )

    @mock.patch("redshift_connector.connect", Mock())
    def test_sslmode_verify_ca(self):
        self.config.credentials.sslmode = "verify-ca"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region="us-east-1",
            timeout=None,
            ssl=True,
            sslmode="verify-ca",
        )

    @mock.patch("redshift_connector.connect", Mock())
    def test_sslmode_prefer(self):
        self.config.credentials.sslmode = "prefer"
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            user="root",
            password="password",
            port=5439,
            auto_create=False,
            db_groups=[],
            region="us-east-1",
            timeout=None,
            ssl=True,
            sslmode="verify-ca",
        )

    @mock.patch("redshift_connector.connect", Mock())
    @mock.patch("boto3.Session", Mock())
    def test_serverless_iam_failure(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
        )
        with self.assertRaises(dbt.exceptions.FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
                database="redshift",
                cluster_identifier=None,
                region="us-east-2",
                auto_create=False,
                db_groups=[],
                db_user="root",
                password="",
                user="",
                profile="test",
                port=5439,
                timeout=None,
                **DEFAULT_SSL_CONFIG,
            )
        self.assertTrue("'host' must be provided" in context.exception.msg)

    def test_iam_conn_optionals(self):
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "redshift",
                    "dbname": "redshift",
                    "user": "root",
                    "host": "thishostshouldnotexist",
                    "port": 5439,
                    "schema": "public",
                    "method": "iam",
                    "cluster_id": "my_redshift",
                    "db_groups": ["my_dbgroup"],
                    "autocreate": True,
                }
            },
            "target": "test",
        }

        config_from_parts_or_dicts(self.config, profile_cfg)

    def test_invalid_auth_method(self):
        # we have to set method this way, otherwise it won't validate
        self.config.credentials.method = "badmethod"
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = RedshiftConnectMethodFactory(self.config.credentials)
            connect_method_factory.get_connect_method()
        self.assertTrue("badmethod" in context.exception.msg)

    def test_invalid_iam_no_cluster_id(self):
        self.config.credentials = self.config.credentials.replace(method="iam")
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = RedshiftConnectMethodFactory(self.config.credentials)
            connect_method_factory.get_connect_method()

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection("master")
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection("master")
        model = mock_connection("model")

        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections.update(
            {
                key: master,
                1: model,
            }
        )
        with mock.patch.object(self.adapter.connections, "add_query") as add_query:
            query_result = mock.MagicMock()
            cursor = mock.Mock()
            cursor.fetchone.return_value = 42
            add_query.side_effect = [(None, cursor), (None, query_result)]

            self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)
            add_query.assert_has_calls(
                [
                    call("select pg_backend_pid()"),
                    call("select pg_terminate_backend(42)"),
                ]
            )

        master.handle.get_backend_pid.assert_not_called()

    def test_dbname_verification_is_case_insensitive(self):
        # Override adapter settings from setUp()
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "redshift",
                    "dbname": "Redshift",
                    "user": "root",
                    "host": "thishostshouldnotexist",
                    "pass": "password",
                    "port": 5439,
                    "schema": "public",
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }
        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.adapter.cleanup_connections()
        self._adapter = RedshiftAdapter(self.config)
        self.adapter.verify_database("redshift")

    def test_execute_with_fetch(self):
        cursor = mock.Mock()
        table = dbt.clients.agate_helper.empty_table()
        with mock.patch.object(self.adapter.connections, "add_query") as mock_add_query:
            mock_add_query.return_value = (
                None,
                cursor,
            )  # when mock_add_query is called, it will always return None, cursor
            with mock.patch.object(self.adapter.connections, "get_response") as mock_get_response:
                mock_get_response.return_value = None
                with mock.patch.object(
                    self.adapter.connections, "get_result_from_cursor"
                ) as mock_get_result_from_cursor:
                    mock_get_result_from_cursor.return_value = table
                    self.adapter.connections.execute(sql="select * from test", fetch=True)
        mock_add_query.assert_called_once_with("select * from test", False)
        mock_get_result_from_cursor.assert_called_once_with(cursor, None)
        mock_get_response.assert_called_once_with(cursor)

    def test_execute_without_fetch(self):
        cursor = mock.Mock()
        with mock.patch.object(self.adapter.connections, "add_query") as mock_add_query:
            mock_add_query.return_value = (
                None,
                cursor,
            )  # when mock_add_query is called, it will always return None, cursor
            with mock.patch.object(self.adapter.connections, "get_response") as mock_get_response:
                mock_get_response.return_value = None
                with mock.patch.object(
                    self.adapter.connections, "get_result_from_cursor"
                ) as mock_get_result_from_cursor:
                    self.adapter.connections.execute(sql="select * from test2", fetch=False)
        mock_add_query.assert_called_once_with("select * from test2", False)
        mock_get_result_from_cursor.assert_not_called()
        mock_get_response.assert_called_once_with(cursor)

    def test_add_query_with_no_cursor(self):
        with mock.patch.object(
            self.adapter.connections, "get_thread_connection"
        ) as mock_get_thread_connection:
            mock_get_thread_connection.return_value = None
            with self.assertRaisesRegex(
                dbt.exceptions.DbtRuntimeError, "Tried to run invalid SQL:  on <None>"
            ):
                self.adapter.connections.add_query(sql="")
        mock_get_thread_connection.assert_called_once()

    def test_add_query_success(self):
        cursor = mock.Mock()
        with mock.patch.object(
            dbt.adapters.redshift.connections.SQLConnectionManager, "add_query"
        ) as mock_add_query:
            mock_add_query.return_value = None, cursor
            self.adapter.connections.add_query("select * from test3")
        mock_add_query.assert_called_once_with(
            "select * from test3", True, bindings=None, abridge_sql_log=False
        )

    def test_query_tagging(self):
        self.config.credentials = self.config.credentials.replace(query_tag="test_query_tag")

        expected_connection_info = [
            (k, v) for (k, v) in self.config.credentials.connection_info() if k == "query_tag"
        ]
        self.assertEqual([("query_tag", "test_query_tag")], expected_connection_info)


class TestRedshiftAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ["", "a1", "stringval1"],
            ["", "a2", "stringvalasdfasdfasdfa"],
            ["", "a3", "stringval3"],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ["varchar(64)", "varchar(2)", "varchar(22)"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ["", "23.98", "-1"],
            ["", "12.78", "-2"],
            ["", "79.41", "-3"],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ["integer", "float8", "integer"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ["", "false", "true"],
            ["", "false", "false"],
            ["", "false", "true"],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ["boolean", "boolean", "boolean"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ["", "20190101T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190102T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190103T01:01:01Z", "2019-01-01 01:01:01"],
        ]
        agate_table = self._make_table_of(
            rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime]
        )
        expected = [
            "timestamp without time zone",
            "timestamp without time zone",
            "timestamp without time zone",
        ]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ["", "2019-01-01", "2019-01-04"],
            ["", "2019-01-02", "2019-01-04"],
            ["", "2019-01-03", "2019-01-04"],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ["date", "date", "date"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_date_type(agate_table, col_idx) == expect

    def test_convert_time_type(self):
        # dbt's default type testers actually don't have a TimeDelta at all.
        rows = [
            ["", "120s", "10s"],
            ["", "3m", "11s"],
            ["", "1h", "12s"],
        ]
        agate_table = self._make_table_of(rows, agate.TimeDelta)
        expected = ["varchar(24)", "varchar(24)", "varchar(24)"]
        for col_idx, expect in enumerate(expected):
            assert RedshiftAdapter.convert_time_type(agate_table, col_idx) == expect
