from multiprocessing import get_context
from unittest import TestCase, mock
from unittest.mock import MagicMock

from dbt.adapters.exceptions import FailedToConnectError
import redshift_connector

from dbt.adapters.redshift import (
    Plugin as RedshiftPlugin,
    RedshiftAdapter,
)
from dbt.adapters.redshift.connections import RedshiftConnectMethodFactory, RedshiftSSLConfig
from tests.unit.utils import config_from_parts_or_dicts, inject_adapter


DEFAULT_SSL_CONFIG = RedshiftSSLConfig().to_dict()


class AuthMethod(TestCase):
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
            self._adapter = RedshiftAdapter(self.config, get_context("spawn"))
            inject_adapter(self._adapter, RedshiftPlugin)
        return self._adapter


class TestInvalidMethod(AuthMethod):
    def test_invalid_auth_method(self):
        # we have to set method this way, otherwise it won't validate
        self.config.credentials.method = "badmethod"
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = RedshiftConnectMethodFactory(self.config.credentials)
            connect_method_factory.get_connect_method()
        self.assertTrue("badmethod" in context.exception.msg)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_missing_region_failure(self):
        # Failure test with no region
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233_no_region",
            region=None,
        )

        with self.assertRaises(FailedToConnectError):
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

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_invalid_region_failure(self):
        # Invalid region test
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233_no_region.us-not-a-region-1",
            region=None,
        )

        with self.assertRaises(FailedToConnectError):
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


class TestDatabaseMethod(AuthMethod):
    @mock.patch("redshift_connector.connect", MagicMock())
    def test_default(self):
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
            region=None,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_explicit_auth_method(self):
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
            region=None,
            timeout=None,
            **DEFAULT_SSL_CONFIG,
        )

    def test_database_verification_is_case_insensitive(self):
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
        self._adapter = RedshiftAdapter(self.config, get_context("spawn"))
        self.adapter.verify_database("redshift")


class TestIAMUserMethod(AuthMethod):

    def test_iam_optionals(self):
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

    def test_no_cluster_id(self):
        self.config.credentials = self.config.credentials.replace(method="iam")
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = RedshiftConnectMethodFactory(self.config.credentials)
            connect_method_factory.get_connect_method()

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_default(self):
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
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            profile=None,
            port=5439,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile(self):
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
            region=None,
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

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_explicit(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            cluster_id="my_redshift",
            host="thishostshouldnotexist.test.us-east-1",
            access_key_id="my_access_key_id",
            secret_access_key="my_secret_access_key",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            access_key_id="my_access_key_id",
            secret_access_key="my_secret_access_key",
            database="redshift",
            db_user="root",
            password="",
            user="",
            cluster_identifier="my_redshift",
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            port=5439,
            **DEFAULT_SSL_CONFIG,
        )


class TestIAMUserMethodServerless(AuthMethod):

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_default_region(self):
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
            region=None,
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

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_explicit_region(self):
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

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_invalid_serverless(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam",
            iam_profile="test",
            host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
                database="redshift",
                cluster_identifier=None,
                region=None,
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


class TestIAMRoleMethod(AuthMethod):

    def test_no_cluster_id(self):
        self.config.credentials = self.config.credentials.replace(method="iam_role")
        with self.assertRaises(FailedToConnectError) as context:
            connect_method_factory = RedshiftConnectMethodFactory(self.config.credentials)
            connect_method_factory.get_connect_method()

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_default(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            cluster_id="my_redshift",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            cluster_identifier="my_redshift",
            db_user=None,
            password="",
            user="",
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            port=5439,
            group_federation=True,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            cluster_id="my_redshift",
            iam_profile="test",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="thishostshouldnotexist.test.us-east-1",
            database="redshift",
            cluster_identifier="my_redshift",
            db_user=None,
            password="",
            user="",
            region=None,
            timeout=None,
            auto_create=False,
            db_groups=[],
            profile="test",
            port=5439,
            group_federation=True,
            **DEFAULT_SSL_CONFIG,
        )


class TestIAMRoleMethodServerless(AuthMethod):
    # Should behave like IAM Role provisioned, with the exception of not having group_federation set

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_default_region(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            cluster_id="my_redshift",
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
        )
        connection = self.adapter.acquire_connection("dummy")
        connection.handle
        redshift_connector.connect.assert_called_once_with(
            iam=True,
            host="doesnotexist.1233.us-east-2.redshift-serverless.amazonaws.com",
            database="redshift",
            cluster_identifier=None,
            region=None,
            auto_create=False,
            db_groups=[],
            db_user=None,
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            # group_federation=False,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_explicit_region(self):
        # Successful test
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
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
            db_user=None,
            password="",
            user="",
            profile="test",
            timeout=None,
            port=5439,
            # group_federation=False,
            **DEFAULT_SSL_CONFIG,
        )

    @mock.patch("redshift_connector.connect", MagicMock())
    def test_profile_invalid_serverless(self):
        self.config.credentials = self.config.credentials.replace(
            method="iam_role",
            iam_profile="test",
            host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
        )
        with self.assertRaises(FailedToConnectError) as context:
            connection = self.adapter.acquire_connection("dummy")
            connection.handle
            redshift_connector.connect.assert_called_once_with(
                iam=True,
                host="doesnotexist.1233.us-east-2.redshift-srvrlss.amazonaws.com",
                database="redshift",
                cluster_identifier=None,
                region=None,
                auto_create=False,
                db_groups=[],
                db_user=None,
                password="",
                user="",
                profile="test",
                port=5439,
                timeout=None,
                # group_federation=False,
                **DEFAULT_SSL_CONFIG,
            )
        self.assertTrue("'host' must be provided" in context.exception.msg)
