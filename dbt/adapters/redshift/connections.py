import re
from multiprocessing import Lock
from contextlib import contextmanager
from typing import Any, Callable, Dict, Tuple, Union, Optional, List, TYPE_CHECKING
from dataclasses import dataclass, field

import sqlparse
import redshift_connector
from dbt.adapters.exceptions import FailedToConnectError
from redshift_connector.utils.oids import get_datatype_name

from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.contracts.util import Replaceable
from dbt_common.dataclass_schema import dbtClassMixin, StrEnum, ValidationError
from dbt_common.helper_types import Port
from dbt_common.exceptions import DbtRuntimeError, CompilationError, DbtDatabaseError

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


class IdentityCenterTokenType(StrEnum):
    ACCESS_TOKEN = "ACCESS_TOKEN"
    EXT_JWT = "EXT_JWT"


class RedshiftConnectionMethod(StrEnum):
    DATABASE = "database"
    IAM = "iam"
    IAM_ROLE = "iam_role"
    IAM_IDENTITY_CENTER_BROWSER = "browser_identity_center"

    @classmethod
    def uses_identity_center(cls, method: str) -> bool:
        return method in (cls.IAM_IDENTITY_CENTER_BROWSER,)

    @classmethod
    def is_iam(cls, method: str) -> bool:
        return not cls.uses_identity_center(method)


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

    #
    # IAM identity center methods
    #

    # browser
    idc_region: Optional[str] = None
    issuer_url: Optional[str] = None
    idp_listen_port: Optional[int] = 7890
    idc_client_display_name: Optional[str] = "Amazon Redshift driver"
    idp_response_timeout: Optional[int] = None

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


def get_connection_method(
    credentials: RedshiftCredentials,
) -> Callable[[], redshift_connector.Connection]:
    #
    # Helper Methods
    #
    def __validate_required_fields(method_name: str, required_fields: Tuple[str, ...]):
        missing_fields: List[str] = [
            field for field in required_fields if getattr(credentials, field, None) is None
        ]
        if missing_fields:
            fields_str: str = "', '".join(missing_fields)
            raise FailedToConnectError(
                f"'{fields_str}' field(s) are required for '{method_name}' credentials method"
            )

    def __base_kwargs(credentials) -> Dict[str, Any]:
        redshift_ssl_config: Dict[str, Any] = RedshiftSSLConfig.parse(
            credentials.sslmode
        ).to_dict()
        return {
            "host": credentials.host,
            "port": int(credentials.port) if credentials.port else 5439,
            "database": credentials.database,
            "region": credentials.region,
            "auto_create": credentials.autocreate,
            "db_groups": credentials.db_groups,
            "timeout": credentials.connect_timeout,
            **redshift_ssl_config,
        }

    def __iam_kwargs(credentials) -> Dict[str, Any]:

        # iam True except for identity center methods
        iam: bool = RedshiftConnectionMethod.is_iam(credentials.method)

        cluster_identifier: Optional[str]
        if "serverless" in credentials.host or RedshiftConnectionMethod.uses_identity_center(
            credentials.method
        ):
            cluster_identifier = None
        elif credentials.cluster_id:
            cluster_identifier = credentials.cluster_id
        else:
            raise FailedToConnectError(
                "Failed to use IAM method:"
                " 'cluster_id' must be provided for provisioned cluster"
                " 'host' must be provided for serverless endpoint"
            )

        iam_specific_kwargs: Dict[str, Any] = {
            "iam": iam,
            "user": "",
            "password": "",
            "cluster_identifier": cluster_identifier,
        }

        return __base_kwargs(credentials) | iam_specific_kwargs

    def __database_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with 'database' credentials method")

        __validate_required_fields("database", ("user", "password"))

        db_credentials: Dict[str, Any] = {
            "user": credentials.user,
            "password": credentials.password,
        }

        return __base_kwargs(credentials) | db_credentials

    def __iam_user_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with 'iam' credentials method")

        iam_credentials: Dict[str, Any]
        if credentials.access_key_id and credentials.secret_access_key:
            iam_credentials = {
                "access_key_id": credentials.access_key_id,
                "secret_access_key": credentials.secret_access_key,
            }
        elif credentials.access_key_id or credentials.secret_access_key:
            raise FailedToConnectError(
                "'access_key_id' and 'secret_access_key' are both needed if providing explicit credentials"
            )
        else:
            iam_credentials = {"profile": credentials.iam_profile}

        __validate_required_fields("iam", ("user",))
        iam_credentials["db_user"] = credentials.user

        return __iam_kwargs(credentials) | iam_credentials

    def __iam_role_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with 'iam_role' credentials method")
        role_kwargs = {
            "db_user": None,
            "group_federation": "serverless" not in credentials.host,
        }

        if credentials.iam_profile:
            role_kwargs["profile"] = credentials.iam_profile

        return __iam_kwargs(credentials) | role_kwargs

    def __iam_idc_browser_kwargs(credentials) -> Dict[str, Any]:
        logger.debug("Connecting to Redshift with '{credentials.method}' credentials method")

        __IDP_TIMEOUT: int = 60
        __LISTEN_PORT_DEFAULT: int = 7890

        __validate_required_fields(
            "browser_identity_center", ("method", "idc_region", "issuer_url")
        )

        idp_timeout: int = (
            timeout
            if (timeout := credentials.idp_response_timeout) or timeout == 0
            else __IDP_TIMEOUT
        )

        idp_listen_port: int = (
            port if (port := credentials.idp_listen_port) else __LISTEN_PORT_DEFAULT
        )

        idc_kwargs: Dict[str, Any] = {
            "credentials_provider": "BrowserIdcAuthPlugin",
            "issuer_url": credentials.issuer_url,
            "listen_port": idp_listen_port,
            "idc_region": credentials.idc_region,
            "idc_client_display_name": credentials.idc_client_display_name,
            "idp_response_timeout": idp_timeout,
        }

        return __iam_kwargs(credentials) | idc_kwargs

    #
    # Head of function execution
    #

    method_to_kwargs_function = {
        None: __database_kwargs,
        RedshiftConnectionMethod.DATABASE: __database_kwargs,
        RedshiftConnectionMethod.IAM: __iam_user_kwargs,
        RedshiftConnectionMethod.IAM_ROLE: __iam_role_kwargs,
        RedshiftConnectionMethod.IAM_IDENTITY_CENTER_BROWSER: __iam_idc_browser_kwargs,
    }

    try:
        kwargs_function: Callable[[RedshiftCredentials], Dict[str, Any]] = (
            method_to_kwargs_function[credentials.method]
        )
    except KeyError:
        raise FailedToConnectError(f"Invalid 'method' in profile: '{credentials.method}'")

    kwargs: Dict[str, Any] = kwargs_function(credentials)

    def connect() -> redshift_connector.Connection:
        c = redshift_connector.connect(**kwargs)
        if credentials.autocommit:
            c.autocommit = True
        if credentials.role:
            c.cursor().execute(f"set role {credentials.role}")
        return c

    return connect


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

        def exponential_backoff(attempt: int):
            return attempt * attempt

        retryable_exceptions = [
            redshift_connector.OperationalError,
            redshift_connector.DatabaseError,
            redshift_connector.DataError,
            redshift_connector.InterfaceError,
        ]

        open_connection = cls.retry_connection(
            connection,
            connect=get_connection_method(credentials),
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
