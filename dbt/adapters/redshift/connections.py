import os
from multiprocessing import Lock
from contextlib import contextmanager
from typing import NewType

import boto3
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.events import AdapterLogger
import dbt.exceptions
import dbt.flags
import redshift_connector
from dbt.exceptions import RuntimeException
from dbt.dataclass_schema import FieldEncoder, dbtClassMixin, StrEnum

from dataclasses import dataclass, field
from typing import Optional, List

from dbt.helper_types import Port
from redshift_connector import OperationalError, DatabaseError, DataError

logger = AdapterLogger("Redshift")

drop_lock: Lock = dbt.flags.MP_CONTEXT.Lock()  # type: ignore


IAMDuration = NewType("IAMDuration", int)


class IAMDurationEncoder(FieldEncoder):
    @property
    def json_schema(self):
        return {"type": "integer", "minimum": 0, "maximum": 65535}


dbtClassMixin.register_field_encoders({IAMDuration: IAMDurationEncoder()})


class RedshiftConnectionMethod(StrEnum):
    DATABASE = "database"
    IAM = "iam"


@dataclass
class RedshiftCredentials(Credentials):
    host: str
    user: str
    port: Port
    method: str = RedshiftConnectionMethod.DATABASE  # type: ignore
    password: Optional[str] = None  # type: ignore
    cluster_id: Optional[str] = field(
        default=None,
        metadata={"description": "If using IAM auth, the name of the cluster"},
    )
    iam_profile: Optional[str] = None
    iam_duration_seconds: int = 900
    search_path: Optional[str] = None  # TODO: Not supported in redshift python connector
    keepalives_idle: int = 4  # TODO: Not supported in redshift python connector
    autocreate: bool = False
    db_groups: List[str] = field(default_factory=list)
    ra3_node: Optional[bool] = False
    connect_timeout: int = 10
    role: Optional[str] = None
    sslmode: Optional[str] = None
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None
    application_name: Optional[str] = "dbt"
    retries: int = 0  # this is in-built into redshift python connector

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        return "host", "port", "user", "database", "schema"

    @property
    def unique_field(self) -> str:
        return self.host


class RedshiftConnectMethodFactory:
    credentials: RedshiftCredentials

    def __init__(self, credentials):
        self.credentials = credentials

    def get_connect_method(self):
        method = self.credentials.method
        # Support missing 'method' for backwards compatibility
        if method == RedshiftConnectionMethod.DATABASE or method is None:
            logger.debug("Connecting to Redshift using 'database' credentials")
            # this requirement is really annoying to encode into json schema,
            # so validate it here
            if self.credentials.password is None:
                raise dbt.exceptions.FailedToConnectException(
                    "'password' field is required for 'database' credentials"
                )

            def connect():
                c = redshift_connector.connect(
                    host=self.credentials.host,
                    database=self.credentials.database,
                    user=self.credentials.user,
                    password=self.credentials.password,
                    port=self.credentials.port if self.credentials.port else 5439,
                    auto_create=self.credentials.autocreate,
                    db_groups=self.credentials.db_groups,
                )
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

            return connect

        elif method == RedshiftConnectionMethod.IAM:
            if not self.credentials.cluster_id:
                raise dbt.exceptions.FailedToConnectException(
                    "Failed to use IAM method, 'cluster_id' must be provided"
                )

            if self.credentials.iam_profile is None:
                return self._get_iam_connect_method_from_env_vars()
            else:
                return self._get_iam_connect_method_with_tmp_cluster_credentials()
        else:
            raise dbt.exceptions.FailedToConnectException(
                "Invalid 'method' in profile: '{}'".format(method)
            )

    def _get_iam_connect_method_from_env_vars(self):
        aws_credentials_env_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]

        def check_if_env_vars_empty(var):
            return os.environ.get(var, "") == ""

        empty_env_vars = list(filter(check_if_env_vars_empty, aws_credentials_env_vars))
        if len(empty_env_vars) > 0:
            raise dbt.exceptions.FailedToConnectException(
                "Failed to specify {} as environment variable(s) in shell".format(empty_env_vars)
            )

        def connect():
            c = redshift_connector.connect(
                iam=True,
                database=self.credentials.database,
                db_user=self.credentials.user,
                password="",
                user="",
                cluster_identifier=self.credentials.cluster_id,
                access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                session_token=os.environ["AWS_SESSION_TOKEN"],
                region=self.credentials.host.split(".")[2],
                auto_create=self.credentials.autocreate,
                db_groups=self.credentials.db_groups,
            )
            if self.credentials.role:
                c.cursor().execute("set role {}".format(self.credentials.role))
            return c

        return connect

    def _get_iam_connect_method_with_tmp_cluster_credentials(self):
        tmp_user, tmp_password = self._get_tmp_iam_cluster_credentials()

        def connect():
            c = redshift_connector.connect(
                iam=True,
                database=self.credentials.database,
                db_user=self.credentials.user,
                password=tmp_password,
                user=tmp_user,
                cluster_identifier=self.credentials.cluster_id,
                region=self.credentials.host.split(".")[2],
                auto_create=self.credentials.autocreate,
                db_groups=self.credentials.db_groups,
            )
            if self.credentials.role:
                c.cursor().execute("set role {}".format(self.credentials.role))
            return c

        return connect

    def _get_tmp_iam_cluster_credentials(self):
        """Fetches temporary login credentials from AWS. The specified user
        must already exist in the database, or else an error will occur"""
        iam_profile = self.credentials.iam_profile
        logger.debug("Connecting to Redshift using 'IAM'" + f"with profile {iam_profile}")
        boto_session = boto3.Session(profile_name=iam_profile)
        boto_client = boto_session.client("redshift")

        try:
            cluster_creds = boto_client.get_cluster_credentials(
                DbUser=self.credentials.user,
                DbName=self.credentials.database,
                ClusterIdentifier=self.credentials.cluster_id,
                DurationSeconds=self.credentials.iam_duration_seconds,
                AutoCreate=self.credentials.autocreate,
                DbGroups=self.credentials.db_groups,
            )
            return cluster_creds.get("DbUser"), cluster_creds.get("DbPassword")
        except boto_client.exceptions.ClientError as e:
            raise dbt.exceptions.FailedToConnectException(
                "Unable to get temporary Redshift cluster credentials: {}".format(e)
            )


class RedshiftConnectionManager(SQLConnectionManager):
    TYPE = "redshift"

    def _get_backend_pid(self):
        sql = "select pg_backend_pid()"
        _, cursor = self.add_query(sql)
        res = cursor.fetchone()
        return res

    def cancel(self, connection: Connection):
        connection_name = connection.name
        try:
            pid = self._get_backend_pid()
            sql = "select pg_terminate_backend({})".format(pid)
            _, cursor = self.add_query(sql)
            res = cursor.fetchone()
            logger.debug("Cancel query '{}': {}".format(connection_name, res))
        except redshift_connector.error.InterfaceError as e:
            if "is closed" in str(e):
                logger.debug(f"Connection {connection_name} was already closed")
                return
            raise

    @classmethod
    def get_response(cls, cursor: redshift_connector.Cursor) -> AdapterResponse:
        rows = cursor.rowcount
        message = f"{rows} cursor.rowcount"
        return AdapterResponse(_message=message, rows_affected=rows)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except redshift_connector.error.Error as e:
            logger.debug(f"Redshift error: {str(e)}")
            self.rollback_if_open()
        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            # Raise DBT native exceptions as is.
            if isinstance(e, dbt.exceptions.Exception):
                raise
            raise RuntimeException(str(e)) from e

    @contextmanager
    def fresh_transaction(self, name=None):
        """On entrance to this context manager, hold an exclusive lock and
        create a fresh transaction for redshift, then commit and begin a new
        one before releasing the lock on exit.

        See drop_relation in RedshiftAdapter for more information.

        :param Optional[str] name: The name of the connection to use, or None
            to use the default.
        """
        with drop_lock:
            connection = self.get_thread_connection()

            if connection.transaction_open:
                self.commit()

            self.begin()
            yield

            self.commit()
            self.begin()

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        connect_method_factory = RedshiftConnectMethodFactory(credentials)

        def exponential_backoff(attempt: int):
            return attempt * attempt

        retryable_exceptions = [OperationalError, DatabaseError, DataError]

        return cls.retry_connection(
            connection,
            connect=connect_method_factory.get_connect_method(),
            logger=logger,
            retry_limit=credentials.retries,
            retry_timeout=exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )
