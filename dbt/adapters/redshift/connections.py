import re
from multiprocessing import Lock
from contextlib import contextmanager
from typing import List, Optional, Tuple, Union
from dataclasses import dataclass, field

import agate
import sqlparse
import redshift_connector
from redshift_connector.utils.oids import get_datatype_name

from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.contracts.util import Replaceable
from dbt.dataclass_schema import dbtClassMixin, StrEnum, ValidationError
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


class UniqueFieldError(CompilationError):
    def __init__(self, exc: ValidationError):
        self.exc = exc
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        validator_msg = self.validator_error_message(self.exc)
        msg = f"Could not parse unique field: {validator_msg}"
        return msg


logger = AdapterLogger("Redshift")


drop_lock: Lock = dbt.flags.MP_CONTEXT.Lock()  # type: ignore


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


NO_DATABASE = "none"


@dataclass
class RedshiftDatabase(dbtClassMixin, Replaceable):  # type: ignore
    database: Optional[str] = None

    @classmethod
    def parse(cls, database: str) -> "RedshiftDatabase":
        raw_redshift_database = {"database": database if database != NO_DATABASE else None}
        redshift_database = cls.from_dict(raw_redshift_database)

        return redshift_database


@dataclass
class RedshiftUniqueField(dbtClassMixin, Replaceable):  # type: ignore
    unique_field: str

    @classmethod
    def parse(
        cls, host: Optional[str] = None, cluster_identifier: Optional[str] = None
    ) -> "RedshiftUniqueField":
        try:
            raw_redshift_unique_field = {"unique_field": host if host else cluster_identifier}
            cls.validate(raw_redshift_unique_field)
        except ValidationError as exc:
            raise UniqueFieldError(exc)

        redshift_unique_field = cls.from_dict(raw_redshift_unique_field)

        return redshift_unique_field


@dataclass
class RedshiftCredentials(Credentials):
    # functional dbt fields
    # schema -> already provided by dbt.contracts.connection.Credentials

    # connection flow fields
    retries: int = 1
    method: Optional[str] = None  # for backwards compatibility
    ra3_node: Optional[bool] = False

    # session specific fields
    # opt-in by default per team deliberation on https://peps.python.org/pep-0249/#autocommit
    autocommit: Optional[bool] = True
    role: Optional[str] = None

    # connection specific fields based on:
    # https://github.com/aws/amazon-redshift-python-driver/blob/v2.0.913/redshift_connector/__init__.py
    user: Optional[str] = None
    # databse already provided by dbt.contracts.connection.Credentials as it is a functional dbt requirement
    password: Optional[str] = None
    port: Optional[Port] = None
    host: Optional[str] = None
    source_address: Optional[str] = None
    unix_sock: Optional[str] = None
    sslmode: Optional[UserSSLMode] = field(default_factory=UserSSLMode.default)
    timeout: Optional[int] = None
    max_prepared_statements: Optional[int] = None
    tcp_keepalive: Optional[bool] = None
    application_name: Optional[str] = None
    replication: Optional[str] = None
    idp_host: Optional[str] = None
    db_user: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    preferred_role: Optional[str] = None
    principal_arn: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    profile: Optional[str] = None
    credentials_provider: Optional[str] = None
    region: Optional[str] = None
    cluster_identifier: Optional[str] = None
    iam: Optional[bool] = None
    client_id: Optional[str] = None
    idp_tenant: Optional[str] = None
    client_secret: Optional[str] = None
    partner_sp_id: Optional[str] = None
    idp_response_timeout: Optional[int] = None
    listen_port: Optional[int] = None
    login_url: Optional[str] = None
    auto_create: Optional[bool] = False
    db_groups: List[str] = field(default_factory=list)
    force_lowercase: Optional[bool] = None
    allow_db_user_override: Optional[bool] = None
    client_protocol_version: Optional[int] = None
    database_metadata_current_db_only: Optional[bool] = None
    ssl_insecure: Optional[bool] = None
    web_identity_token: Optional[str] = None
    role_session_name: Optional[str] = None
    role_arn: Optional[str] = None
    iam_disable_cache: Optional[bool] = None
    auth_profile: Optional[str] = None
    endpoint_url: Optional[str] = None
    provider_name: Optional[str] = None
    scope: Optional[str] = None
    numeric_to_float: Optional[bool] = False
    is_serverless: Optional[bool] = False
    serverless_acct_id: Optional[str] = None
    serverless_work_group: Optional[str] = None
    group_federation: Optional[bool] = None

    _ALIASES = {
        "dbname": "database",
        "pass": "password",
        # for backwards compatibility
        "auto_create": "autocreate",
        "cluster_identifier": "cluster_id",
        "connect_timeout": "timeout",
        "iam_profile": "profile",
    }

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        return (
            "schema",
            "autocommit",
            "role",
            "method",
            "ra3_node",
            "retries",
            "database",
            "user",
            "password",
            "port",
            "host",
            "source_address",
            "unix_sock",
            "sslmode",
            "timeout",
            "max_prepared_statements",
            "tcp_keepalive",
            "application_name",
            "replication",
            "idp_host",
            "db_user",
            "app_id",
            "app_name",
            "preferred_role",
            "principal_arn",
            "access_key_id",
            "secret_access_key",
            "session_token",
            "profile",
            "credentials_provider",
            "region",
            "cluster_identifier",
            "iam",
            "client_id",
            "idp_tenant",
            "client_secret",
            "partner_sp_id",
            "idp_response_timeout",
            "listen_port",
            "login_url",
            "auto_create",
            "db_groups",
            "force_lowercase",
            "allow_db_user_override",
            "client_protocol_version",
            "database_metadata_current_db_only",
            "ssl_insecure",
            "web_identity_token",
            "role_session_name",
            "role_arn",
            "iam_disable_cache",
            "auth_profile",
            "endpoint_url",
            "provider_name",
            "scope",
            "numeric_to_float",
            "is_serverless",
            "serverless_acct_id",
            "serverless_work_group",
            "group_federation",
        )

    @property
    def unique_field(self) -> str:
        return RedshiftUniqueField.parse(self.host, self.cluster_identifier).unique_field


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

        def exponential_backoff(attempt: int):
            return attempt * attempt

        retryable_exceptions = [
            redshift_connector.OperationalError,
            redshift_connector.DatabaseError,
            redshift_connector.DataError,
        ]

        kwargs = {
            k: v
            for k, v in {
                "user": credentials.user,
                "password": credentials.password,
                "port": int(credentials.port) if credentials.port else None,
                "host": credentials.host,
                "source_address": credentials.source_address,
                "unix_sock": credentials.unix_sock,
                "timeout": credentials.timeout,
                "max_prepared_statements": credentials.max_prepared_statements,
                "tcp_keepalive": credentials.tcp_keepalive,
                "application_name": credentials.application_name,
                "replication": credentials.replication,
                "idp_host": credentials.idp_host,
                "db_user": credentials.db_user,
                "app_id": credentials.app_id,
                "app_name": credentials.app_name,
                "preferred_role": credentials.preferred_role,
                "principal_arn": credentials.principal_arn,
                "access_key_id": credentials.access_key_id,
                "secret_access_key": credentials.secret_access_key,
                "session_token": credentials.session_token,
                "profile": credentials.profile,
                "credentials_provider": credentials.credentials_provider,
                "region": credentials.region,
                "cluster_identifier": credentials.cluster_identifier,
                "iam": credentials.iam,
                "client_id": credentials.client_id,
                "idp_tenant": credentials.idp_tenant,
                "client_secret": credentials.client_secret,
                "partner_sp_id": credentials.partner_sp_id,
                "idp_response_timeout": credentials.idp_response_timeout,
                "listen_port": credentials.listen_port,
                "login_url": credentials.login_url,
                "auto_create": credentials.auto_create,
                "db_groups": credentials.db_groups,
                "force_lowercase": credentials.force_lowercase,
                "allow_db_user_override": credentials.allow_db_user_override,
                "client_protocol_version": credentials.client_protocol_version,
                "database_metadata_current_db_only": credentials.database_metadata_current_db_only,
                "ssl_insecure": credentials.ssl_insecure,
                "web_identity_token": credentials.web_identity_token,
                "role_session_name": credentials.role_session_name,
                "role_arn": credentials.role_arn,
                "iam_disable_cache": credentials.iam_disable_cache,
                "auth_profile": credentials.auth_profile,
                "endpoint_url": credentials.endpoint_url,
                "provider_name": credentials.provider_name,
                "scope": credentials.scope,
                "numeric_to_float": credentials.numeric_to_float,
                "is_serverless": credentials.is_serverless,
                "serverless_acct_id": credentials.serverless_acct_id,
                "serverless_work_group": credentials.serverless_work_group,
                "group_federation": credentials.group_federation,
            }.items()
            if v is not None
        }

        # for redshift_connector database is not required and can be provided by an authentication profile
        redshift_database = RedshiftDatabase.parse(credentials.database)
        kwargs.update(redshift_database.to_dict())

        redshift_ssl_config = RedshiftSSLConfig.parse(credentials.sslmode)
        kwargs.update(redshift_ssl_config.to_dict())

        # for backwards compatibility
        if credentials.method == RedshiftConnectionMethod.IAM:
            kwargs.update(
                {
                    "iam": True,
                    "db_user": credentials.user,
                    "user": "",
                    "password": "",
                }
            )

        def connect():
            c = redshift_connector.connect(
                **kwargs,
            )
            if credentials.autocommit:
                c.autocommit = True
            if credentials.role:
                c.cursor().execute("set role {}".format(credentials.role))
            return c

        return cls.retry_connection(
            connection,
            connect,
            logger=logger,
            retry_limit=credentials.retries,
            retry_timeout=exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
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
