import re
from multiprocessing import Lock
from contextlib import contextmanager
from typing import NewType, Tuple, Union, Optional, List
from dataclasses import dataclass, field

import agate
import sqlparse
import redshift_connector
from redshift_connector.utils.oids import get_datatype_name

from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.contracts.util import Replaceable
from dbt.dataclass_schema import FieldEncoder, dbtClassMixin, StrEnum, ValidationError
from dbt.events import AdapterLogger
from dbt.exceptions import DbtRuntimeError, CompilationError
import dbt.flags
from dbt.helper_types import Port


class SSLConfigError(CompilationError):
    def __init__(self, exc: ValidationError):
        self.exc = exc
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        validator_msg = self.validator_error_message(self.exc)
        msg = f"Could not parse SSL config: {validator_msg}"
        return msg


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


class UserSSLMode(StrEnum):
    disable = "disable"
    allow = "allow"
    prefer = "prefer"
    require = "require"
    verify_ca = "verify-ca"
    verify_full = "verify-full"

    @classmethod
    def default(cls) -> "UserSSLMode":
        # default for `psycopg2`, which aligns with dbt-redshift 1.4 and provides backwards compatibility
        return cls.prefer


class RedshiftSSLMode(StrEnum):
    verify_ca = "verify-ca"
    verify_full = "verify-full"


SSL_MODE_TRANSLATION = {
    UserSSLMode.disable: None,
    UserSSLMode.allow: RedshiftSSLMode.verify_ca,
    UserSSLMode.prefer: RedshiftSSLMode.verify_ca,
    UserSSLMode.require: RedshiftSSLMode.verify_ca,
    UserSSLMode.verify_ca: RedshiftSSLMode.verify_ca,
    UserSSLMode.verify_full: RedshiftSSLMode.verify_full,
}


@dataclass
class RedshiftSSLConfig(dbtClassMixin, Replaceable):  # type: ignore
    ssl: bool = True
    sslmode: Optional[RedshiftSSLMode] = SSL_MODE_TRANSLATION[UserSSLMode.default()]

    @classmethod
    def parse(cls, user_sslmode: UserSSLMode) -> "RedshiftSSLConfig":
        try:
            raw_redshift_ssl = {
                "ssl": user_sslmode != UserSSLMode.disable,
                "sslmode": SSL_MODE_TRANSLATION[user_sslmode],
            }
            cls.validate(raw_redshift_ssl)
        except ValidationError as exc:
            raise SSLConfigError(exc)

        redshift_ssl = cls.from_dict(raw_redshift_ssl)

        if redshift_ssl.ssl:
            message = (
                f"Establishing connection using ssl with `sslmode` set to '{user_sslmode}'."
                f"To connect without ssl, set `sslmode` to 'disable'."
            )
        else:
            message = "Establishing connection without ssl."

        logger.debug(message)

        return redshift_ssl


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
    connect_timeout: Optional[int] = None
    role: Optional[str] = None
    sslmode: Optional[UserSSLMode] = field(default_factory=UserSSLMode.default)
    retries: int = 1
    region: Optional[str] = None
    # opt-in by default per team deliberation on https://peps.python.org/pep-0249/#autocommit
    autocommit: Optional[bool] = True

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        return (
            "host",
            "port",
            "user",
            "database",
            "schema",
            "method",
            "cluster_id",
            "iam_profile",
            "sslmode",
            "region",
        )

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
            "port": int(self.credentials.port) if self.credentials.port else int(5439),
            "auto_create": self.credentials.autocreate,
            "db_groups": self.credentials.db_groups,
            "region": self.credentials.region,
            "timeout": self.credentials.connect_timeout,
        }

        redshift_ssl_config = RedshiftSSLConfig.parse(self.credentials.sslmode)
        kwargs.update(redshift_ssl_config.to_dict())

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
                    user=self.credentials.user,
                    password=self.credentials.password,
                    **kwargs,
                )
                if self.credentials.autocommit:
                    c.autocommit = True
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

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
                if self.credentials.autocommit:
                    c.autocommit = True
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

        else:
            raise dbt.exceptions.FailedToConnectError(
                "Invalid 'method' in profile: '{}'".format(method)
            )

        return connect


class RedshiftConnectionManager(SQLConnectionManager):
    TYPE = "redshift"

    def _get_backend_pid(self):
        sql = "select pg_backend_pid()"
        _, cursor = self.add_query(sql)
        res = cursor.fetchone()
        return res

    def cancel(self, connection: Connection):
        try:
            pid = self._get_backend_pid()
        except redshift_connector.InterfaceError as e:
            if "is closed" in str(e):
                logger.debug(f"Connection {connection.name} was already closed")
                return
            raise

        sql = f"select pg_terminate_backend({pid})"
        _, cursor = self.add_query(sql)
        res = cursor.fetchone()
        logger.debug(f"Cancel query '{connection.name}': {res}")

    @classmethod
    def get_response(cls, cursor: redshift_connector.Cursor) -> AdapterResponse:
        # redshift_connector.Cursor doesn't have a status message attribute but
        # this function is only used for successful run, so we can just return a dummy
        rows = cursor.rowcount
        message = "SUCCESS"
        return AdapterResponse(_message=message, rows_affected=rows)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except redshift_connector.DatabaseError as e:
            try:
                err_msg = e.args[0]["M"]  # this is a type redshift sets, so we must use these keys
            except Exception:
                err_msg = str(e).strip()
            logger.debug(f"Redshift error: {err_msg}")
            self.rollback_if_open()
            raise dbt.exceptions.DbtDatabaseError(err_msg) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            # Raise DBT native exceptions as is.
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                raise
            raise dbt.exceptions.DbtRuntimeError(str(e)) from e

    @contextmanager
    def fresh_transaction(self):
        """On entrance to this context manager, hold an exclusive lock and
        create a fresh transaction for redshift, then commit and begin a new
        one before releasing the lock on exit.

        See drop_relation in RedshiftAdapter for more information.
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

        retryable_exceptions = [
            redshift_connector.OperationalError,
            redshift_connector.DatabaseError,
            redshift_connector.DataError,
        ]

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
            raise DbtRuntimeError(f"Tried to run invalid SQL: {sql} on {conn_name}")

        return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        return get_datatype_name(type_code)
