import os
from multiprocessing import Lock
from contextlib import contextmanager
from typing import NewType, Any

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
    search_path: Optional[str] = None
    keepalives_idle: int = 4
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
    retries: int = 1

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        keys = super()._connection_keys()
        return keys + ("method", "cluster_id", "iam_profile", "iam_duration_seconds")

    @property
    def unique_field(self) -> str:
        return self.host


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
    def get_response(cls, cursor: Any) -> AdapterResponse:
        message = str(cursor.statusmessage)
        rows = cursor.rowcount
        status_message_parts = message.split() if message is not None else []
        status_message_strings = [part for part in status_message_parts if not part.isdigit()]
        code = " ".join(status_message_strings)
        return AdapterResponse(_message=message, code=code, rows_affected=rows)

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

    @staticmethod
    def _get_connect_method(credentials):
        method = credentials.method
        # Support missing 'method' for backwards compatibility
        if method == "database" or method is None:
            logger.debug("Connecting to Redshift using 'database' credentials")
            # this requirement is really annoying to encode into json schema,
            # so validate it here
            if credentials.password is None:
                raise dbt.exceptions.FailedToConnectException(
                    "'password' field is required for 'database' credentials"
                )

            def connect():
                c = redshift_connector.connect(
                    host=credentials.host,
                    database=credentials.database,
                    user=credentials.user,
                    password=credentials.password,
                    port=credentials.port if credentials.port else 5439,
                )
                if credentials.role:
                    c.cursor().execute("set role {}".format(credentials.role))
                return c

            return connect

        elif method == "iam":

            def connect():
                c = redshift_connector.connect(
                    iam=True,
                    database=credentials.database,
                    db_user=credentials.user,
                    password="",
                    user="",
                    cluster_identifier=credentials.cluster_id,
                    access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                    secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                    session_token=os.environ["AWS_SESSION_TOKEN"],
                    region=credentials.host.split(".")[2],
                )
                if credentials.role:
                    c.cursor().execute("set role {}".format(credentials.role))
                return c

            return connect
        else:
            raise dbt.exceptions.FailedToConnectException(
                "Invalid 'method' in profile: '{}'".format(method)
            )

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        def exponential_backoff(attempt: int):
            return attempt * attempt

        retryable_exceptions = [OperationalError, DatabaseError, DataError]

        return cls.retry_connection(
            connection,
            connect=cls._get_connect_method(credentials),
            logger=logger,
            retry_limit=credentials.retries,
            retry_timeout=exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )
