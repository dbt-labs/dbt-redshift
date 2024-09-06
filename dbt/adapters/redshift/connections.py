import re
from multiprocessing import Lock
from contextlib import contextmanager
from typing import Any, Callable, Dict, Tuple, Union, Optional, List, TYPE_CHECKING
from dataclasses import dataclass, field
import time

import sqlparse
import redshift_connector
from dbt.adapters.exceptions import FailedToConnectError
from redshift_connector.utils.oids import get_datatype_name

from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import SQLQuery, SQLQueryStatus
from dbt_common.contracts.util import Replaceable
from dbt_common.dataclass_schema import dbtClassMixin, StrEnum, ValidationError
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.helper_types import Port
from dbt_common.exceptions import DbtRuntimeError, CompilationError, DbtDatabaseError
from dbt_common.utils import cast_to_str

if TYPE_CHECKING:
    # Indirectly imported via agate_helper, which is lazy loaded further downfile.
    # Used by mypy for earlier type hints.
    import agate


class SSLConfigError(CompilationError):
    def __init__(self, exc: ValidationError):
        self.exc = exc
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        validator_msg = self.validator_error_message(self.exc)
        msg = f"Could not parse SSL config: {validator_msg}"
        return msg


logger = AdapterLogger("Redshift")


class RedshiftConnectionMethod(StrEnum):
    DATABASE = "database"
    IAM = "iam"
    IAM_ROLE = "iam_role"


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
        return cls("prefer")


class RedshiftSSLMode(StrEnum):
    verify_ca = "verify-ca"
    verify_full = "verify-full"


SSL_MODE_TRANSLATION = {
    UserSSLMode.disable: None,
    UserSSLMode.allow: RedshiftSSLMode("verify-ca"),
    UserSSLMode.prefer: RedshiftSSLMode("verify-ca"),
    UserSSLMode.require: RedshiftSSLMode("verify-ca"),
    UserSSLMode.verify_ca: RedshiftSSLMode("verify-ca"),
    UserSSLMode.verify_full: RedshiftSSLMode("verify-full"),
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
    port: Port
    method: str = RedshiftConnectionMethod.DATABASE  # type: ignore
    user: Optional[str] = None
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
    sslmode: UserSSLMode = field(default_factory=UserSSLMode.default)
    retries: int = 1
    region: Optional[str] = None
    # opt-in by default per team deliberation on https://peps.python.org/pep-0249/#autocommit
    autocommit: Optional[bool] = True
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        return (
            "host",
            "user",
            "port",
            "database",
            "method",
            "cluster_id",
            "iam_profile",
            "schema",
            "sslmode",
            "region",
            "sslmode",
            "region",
            "autocreate",
            "db_groups",
            "ra3_node",
            "connect_timeout",
            "role",
            "retries",
            "autocommit",
            "access_key_id",
        )

    @property
    def unique_field(self) -> str:
        return self.host


class RedshiftConnectMethodFactory:
    credentials: RedshiftCredentials

    def __init__(self, credentials) -> None:
        self.credentials = credentials

    def get_connect_method(self) -> Callable[[], redshift_connector.Connection]:

        # Support missing 'method' for backwards compatibility
        method = self.credentials.method or RedshiftConnectionMethod.DATABASE
        if method == RedshiftConnectionMethod.DATABASE:
            kwargs = self._database_kwargs
        elif method == RedshiftConnectionMethod.IAM:
            kwargs = self._iam_user_kwargs
        elif method == RedshiftConnectionMethod.IAM_ROLE:
            kwargs = self._iam_role_kwargs
        else:
            raise FailedToConnectError(f"Invalid 'method' in profile: '{method}'")

        def connect() -> redshift_connector.Connection:
            c = redshift_connector.connect(**kwargs)
            if self.credentials.autocommit:
                c.autocommit = True
            if self.credentials.role:
                c.cursor().execute(f"set role {self.credentials.role}")
            return c

        return connect

    @property
    def _database_kwargs(self) -> Dict[str, Any]:
        logger.debug("Connecting to redshift with 'database' credentials method")
        kwargs = self._base_kwargs

        if self.credentials.user and self.credentials.password:
            kwargs.update(
                user=self.credentials.user,
                password=self.credentials.password,
            )
        else:
            raise FailedToConnectError(
                "'user' and 'password' fields are required for 'database' credentials method"
            )

        return kwargs

    @property
    def _iam_user_kwargs(self) -> Dict[str, Any]:
        logger.debug("Connecting to redshift with 'iam' credentials method")
        kwargs = self._iam_kwargs

        if self.credentials.access_key_id and self.credentials.secret_access_key:
            kwargs.update(
                access_key_id=self.credentials.access_key_id,
                secret_access_key=self.credentials.secret_access_key,
            )
        elif self.credentials.access_key_id or self.credentials.secret_access_key:
            raise FailedToConnectError(
                "'access_key_id' and 'secret_access_key' are both needed if providing explicit credentials"
            )
        else:
            kwargs.update(profile=self.credentials.iam_profile)

        if user := self.credentials.user:
            kwargs.update(db_user=user)
        else:
            raise FailedToConnectError("'user' field is required for 'iam' credentials method")

        return kwargs

    @property
    def _iam_role_kwargs(self) -> Dict[str, Optional[Any]]:
        logger.debug("Connecting to redshift with 'iam_role' credentials method")
        kwargs = self._iam_kwargs

        # It's a role, we're ignoring the user
        kwargs.update(db_user=None)

        # Serverless shouldn't get group_federation, Provisoned clusters should
        if "serverless" in self.credentials.host:
            kwargs.update(group_federation=False)
        else:
            kwargs.update(group_federation=True)

        if iam_profile := self.credentials.iam_profile:
            kwargs.update(profile=iam_profile)

        return kwargs

    @property
    def _iam_kwargs(self) -> Dict[str, Any]:
        kwargs = self._base_kwargs
        kwargs.update(
            iam=True,
            user="",
            password="",
        )

        if "serverless" in self.credentials.host:
            kwargs.update(cluster_identifier=None)
        elif cluster_id := self.credentials.cluster_id:
            kwargs.update(cluster_identifier=cluster_id)
        else:
            raise FailedToConnectError(
                "Failed to use IAM method:"
                "    'cluster_id' must be provided for provisioned cluster"
                "    'host' must be provided for serverless endpoint"
            )

        return kwargs

    @property
    def _base_kwargs(self) -> Dict[str, Any]:
        kwargs = {
            "host": self.credentials.host,
            "port": int(self.credentials.port) if self.credentials.port else int(5439),
            "database": self.credentials.database,
            "region": self.credentials.region,
            "auto_create": self.credentials.autocreate,
            "db_groups": self.credentials.db_groups,
            "timeout": self.credentials.connect_timeout,
        }
        redshift_ssl_config = RedshiftSSLConfig.parse(self.credentials.sslmode)
        kwargs.update(redshift_ssl_config.to_dict())
        return kwargs


class RedshiftConnectionManager(SQLConnectionManager):
    TYPE = "redshift"

    def cancel(self, connection: Connection):
        pid = connection.backend_pid  # type: ignore
        sql = f"select pg_terminate_backend({pid})"
        logger.debug(f"Cancel query on: '{connection.name}' with PID: {pid}")
        logger.debug(sql)

        try:
            self.add_query(sql)
        except redshift_connector.InterfaceError as e:
            if "is closed" in str(e):
                logger.debug(f"Connection {connection.name} was already closed")
                return
            raise

    @classmethod
    def _get_backend_pid(cls, connection):
        with connection.handle.cursor() as c:
            sql = "select pg_backend_pid()"
            res = c.execute(sql).fetchone()
        return res[0]

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
            raise DbtDatabaseError(err_msg) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            # Raise DBT native exceptions as is.
            if isinstance(e, DbtRuntimeError):
                raise
            raise DbtRuntimeError(str(e)) from e

    @contextmanager
    def fresh_transaction(self):
        """On entrance to this context manager, hold an exclusive lock and
        create a fresh transaction for redshift, then commit and begin a new
        one before releasing the lock on exit.

        See drop_relation in RedshiftAdapter for more information.
        """
        drop_lock: Lock = self.lock

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

        open_connection = cls.retry_connection(
            connection,
            connect=connect_method_factory.get_connect_method(),
            logger=logger,
            retry_limit=credentials.retries,
            retry_timeout=exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )
        open_connection.backend_pid = cls._get_backend_pid(open_connection)  # type: ignore
        return open_connection

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            from dbt_common.clients import agate_helper

            table = agate_helper.empty_table()
        return response, table

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):
        connection = None
        cursor = None

        self._initialize_sqlparse_lexer()
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

    @staticmethod
    def _initialize_sqlparse_lexer():
        """
        Resolves: https://github.com/dbt-labs/dbt-redshift/issues/710
        Implementation of this fix: https://github.com/dbt-labs/dbt-core/pull/8215
        """
        from sqlparse.lexer import Lexer  # type: ignore

        if hasattr(Lexer, "get_default_instance"):
            Lexer.get_default_instance()

    def columns_in_relation(self, relation) -> List[Dict[str, Any]]:
        connection = self.get_thread_connection()

        fire_event(
            SQLQuery(
                conn_name=cast_to_str(connection.name),
                sql=f"call redshift_connector.Connection.get_columns({relation.database}, {relation.schema}, {relation.identifier})",
                node_info=get_node_info(),
            )
        )

        pre = time.perf_counter()

        cursor = connection.handle.cursor()
        columns = cursor.get_columns(
            catalog=relation.database,
            schema_pattern=relation.schema,
            tablename_pattern=relation.identifier,
        )

        fire_event(
            SQLQueryStatus(
                status=str(self.get_response(cursor)),
                elapsed=time.perf_counter() - pre,
                node_info=get_node_info(),
            )
        )

        return [self._parse_column_results(column) for column in columns]

    @staticmethod
    def _parse_column_results(record: Tuple[Any, ...]) -> Dict[str, Any]:
        # column positions in the tuple
        column_name = 3
        dtype_code = 4
        dtype_name = 5
        column_size = 6
        decimals = 8

        char_dtypes = [1, 12]
        num_dtypes = [2, 3, 4, 5, 6, 7, 8]
        return {
            "column": record[column_name],
            "dtype": record[dtype_name],
            "char_size": record[column_size] if record[dtype_code] in char_dtypes else None,
            "numeric_precision": record[column_size] if record[dtype_code] in num_dtypes else None,
            "numeric_scale": record[decimals] if record[dtype_code] in num_dtypes else None,
        }
