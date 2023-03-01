import re
from multiprocessing import Lock
from contextlib import contextmanager
from typing import NewType, Tuple

import agate
import sqlparse
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.events import AdapterLogger
import dbt.exceptions
import dbt.flags
import redshift_connector
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
    autocreate: bool = False
    db_groups: List[str] = field(default_factory=list)
    ra3_node: Optional[bool] = False
    connect_timeout: int = 30
    role: Optional[str] = None
    sslmode: Optional[str] = None
    retries: int = 1

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        return "host", "port", "user", "database", "schema", "method", "cluster_id", "iam_profile"

    @property
    def unique_field(self) -> str:
        return self.host


class RedshiftConnectMethodFactory:
    credentials: RedshiftCredentials

    def __init__(self, credentials):
        self.credentials = credentials

    def get_connect_method(self):
        method = self.credentials.method
        kwargs = {
            "host": self.credentials.host,
            "database": self.credentials.database,
            "port": self.credentials.port if self.credentials.port else 5439,
            "auto_create": self.credentials.autocreate,
            "db_groups": self.credentials.db_groups,
            "region": self.credentials.host.split(".")[2],
            "timeout": self.credentials.connect_timeout,
        }
        if self.credentials.sslmode:
            kwargs["sslmode"] = self.credentials.sslmode

        # Support missing 'method' for backwards compatibility
        if method == RedshiftConnectionMethod.DATABASE or method is None:
            # this requirement is really annoying to encode into json schema,
            # so validate it here
            if self.credentials.password is None:
                raise dbt.exceptions.FailedToConnectError(
                    "'password' field is required for 'database' credentials"
                )

            def connect():
                logger.debug("Connecting to redshift with username/password based auth...")
                c = redshift_connector.connect(
                    user=self.credentials.user, password=self.credentials.password, **kwargs
                )
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

            return connect

        elif method == RedshiftConnectionMethod.IAM:
            if not self.credentials.cluster_id and "serverless" not in self.credentials.host:
                raise dbt.exceptions.FailedToConnectError(
                    "Failed to use IAM method. 'cluster_id' must be provided for provisioned cluster. "
                    "'host' must be provided for serverless endpoint."
                )

            def connect():
                logger.debug("Connecting to redshift with IAM based auth...")
                c = redshift_connector.connect(
                    iam=True,
                    db_user=self.credentials.user,
                    password="",
                    user="",
                    cluster_identifier=self.credentials.cluster_id,
                    profile=self.credentials.iam_profile,
                    **kwargs,
                )
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

            return connect
        else:
            raise dbt.exceptions.FailedToConnectError(
                "Invalid 'method' in profile: '{}'".format(method)
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
        message = f"cursor.rowcount = {rows}"
        return AdapterResponse(_message=message, rows_affected=rows)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except redshift_connector.error.DatabaseError as e:
            logger.debug(f"Redshift error: {str(e)}")
            self.rollback_if_open()
            raise dbt.exceptions.DbtDatabaseError(str(e))
        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            # Raise DBT native exceptions as is.
            if isinstance(e, dbt.exceptions.Exception):
                raise
            raise dbt.exceptions.DbtRuntimeError(str(e)) from e

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

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[AdapterResponse, agate.Table]:
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return response, table

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):
        connection = None
        cursor = None

        queries = sqlparse.split(sql)

        for query in queries:
            # Strip off comments from the current query
            without_comments = re.sub(
                re.compile(r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)", re.MULTILINE),
                "",
                query,
            ).strip()

            if without_comments == "":
                continue

            connection, cursor = super().add_query(
                query, auto_begin, bindings=bindings, abridge_sql_log=abridge_sql_log
            )

        if cursor is None:
            conn = self.get_thread_connection()
            conn_name = conn.name if conn and conn.name else "<None>"
            raise dbt.exceptions.DbtRuntimeError(f"Tried to run invalid SQL: {sql} on {conn_name}")

        return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials
