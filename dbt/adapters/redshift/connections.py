import os
import re
from multiprocessing import Lock
from contextlib import contextmanager
from typing import NewType, Tuple, Optional, List
from dataclasses import dataclass, field


import agate
import sqlparse
import redshift_connector

import urllib.request
import json
from redshift_connector.utils.oids import get_datatype_name

from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, Credentials
from dbt.events import AdapterLogger
import dbt.exceptions
import dbt.flags
from dbt.dataclass_schema import FieldEncoder, dbtClassMixin, StrEnum
from dbt.helper_types import Port

logger = AdapterLogger("Redshift")

drop_lock: Lock = dbt.flags.MP_CONTEXT.Lock()  # type: ignore

IAMDuration = NewType("IAMDuration", int)


class IAMDurationEncoder(FieldEncoder):
    @property
    def json_schema(self):
        return {"type": "integer", "minimum": 0, "maximum": 65535}


dbtClassMixin.register_field_encoders({IAMDuration: IAMDurationEncoder()})


def _get_aws_regions():
    # Extract the prefixes from the AWS IP ranges JSON to determine the available regions
    url = "https://ip-ranges.amazonaws.com/ip-ranges.json"
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode())
    regions = set()

    for prefix in data["prefixes"]:
        if prefix["service"] == "AMAZON":
            regions.add(prefix["region"])

    return regions


_AVAILABLE_AWS_REGIONS = _get_aws_regions()


class RedshiftConnectionMethod(StrEnum):
    DATABASE = "database"
    IAM = "iam"
    AUTH_PROFILE = "auth_profile"
    IDP = "IdP"


@dataclass
class RedshiftCredentials(Credentials):
    port: Port
    region: Optional[str] = None
    host: Optional[str] = None
    user: Optional[str] = None
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
    auth_profile: Optional[str] = None
    # Azure identity provider plugin
    credentials_provider: Optional[str] = None
    azure_idp_tenant: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    azure_preferred_role: Optional[str] = None
    # Okta identity provider plugin
    okta_idp_host: Optional[str] = None
    okta_app_id: Optional[str] = None
    okta_app_name: Optional[str] = None

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
        return self.host if self.host else self.database


def _is_valid_region(region):
    if region is None or len(region) == 0:
        logger.warning("Couldn't determine AWS regions. Skipping validation to avoid blocking.")
        return True
    return region in _AVAILABLE_AWS_REGIONS


class RedshiftConnectMethodFactory:
    credentials: RedshiftCredentials

    def __init__(self, credentials):
        self.credentials = credentials

    def get_connect_method(self):
        method = self.credentials.method
        kwargs = {
            "host": None,
            "region": self.credentials.region,
            "database": self.credentials.database,
            "port": self.credentials.port if self.credentials.port else 5439,
            "auto_create": self.credentials.autocreate,
            "db_groups": self.credentials.db_groups,
            "timeout": self.credentials.connect_timeout,
            "application_name": "dbt",
        }
        if method == RedshiftConnectionMethod.IAM or method == RedshiftConnectionMethod.DATABASE:
            kwargs["host"] = self.credentials.host
        if kwargs["region"] is None:
            logger.debug("No region provided, attempting to determine from host.")
            try:
                region_value = self.credentials.host.split(".")[2]
            except IndexError:
                raise dbt.exceptions.FailedToConnectError(
                    "No region provided and unable to determine region from host: "
                    "{}".format(self.credentials.host)
                )

            kwargs["region"] = region_value

        # Validate the set region
        if not _is_valid_region(kwargs["region"]):
            raise dbt.exceptions.FailedToConnectError(
                "Invalid region provided: {}".format(kwargs["region"])
            )

        if self.credentials.sslmode:
            kwargs["sslmode"] = self.credentials.sslmode
        if self.credentials.host and self.credentials.region:
            if self.credentials.host.split(".")[2] != self.credentials.region:
                raise dbt.exceptions.FailedToConnectError(
                    "'region' provided in profiles.yml does not match with region of 'host'."
                )

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
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

        elif method == RedshiftConnectionMethod.AUTH_PROFILE:
            if not self.credentials.auth_profile:
                raise dbt.exceptions.FailedToConnectError(
                    "Failed to use auth profile method. 'auth_profile' must be provided."
                )
            if not self.credentials.region:
                raise dbt.exceptions.FailedToConnectError(
                    "Failed to use auth profile method. 'region' must be provided."
                )

            def connect():
                logger.debug("Connecting to redshift with authentication profile...")
                c = redshift_connector.connect(
                    iam=True,
                    access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                    secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                    session_token=os.environ["AWS_SESSION_TOKEN"],
                    db_user=self.credentials.user,
                    auth_profile=self.credentials.auth_profile,
                    **kwargs,
                )
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

        elif method == RedshiftConnectionMethod.IDP:
            if not self.credentials.credentials_provider:
                raise dbt.exceptions.FailedToConnectError(
                    "Failed to use IdP credentials. 'credentials_provider' must be provided."
                )

            if not self.credentials.region:
                raise dbt.exceptions.FailedToConnectError(
                    "Failed to use IdP credentials. 'region' must be provided."
                )

            if not self.credentials.password or not self.credentials.user:
                raise dbt.exceptions.FailedToConnectError(
                    "Failed to use IdP credentials. 'password' and 'user' must be provided."
                )

            if (
                (self.credentials.credentials_provider.lower() != "azurecredentialsprovider")
                and (self.credentials.credentials_provider.lower() != "azure")
                and (self.credentials.credentials_provider.lower() != "okta")
                and (self.credentials.credentials_provider.lower() != "oktacredentialsprovider")
            ):
                raise dbt.exceptions.FailedToConnectError(
                    "Unrecognized credentials provider. Enter 'azure' or 'okta' for 'credentials_provider'."
                )

            if (self.credentials.credentials_provider.lower() == "azurecredentialsprovider") or (
                self.credentials.credentials_provider.lower() == "azure"
            ):
                if (
                    not self.credentials.azure_idp_tenant
                    or not self.credentials.azure_client_id
                    or not self.credentials.azure_client_secret
                    or not self.credentials.azure_preferred_role
                ):
                    raise dbt.exceptions.FailedToConnectError(
                        "Failed to use Azure credential. 'azure_idp_tenant', 'azure_client_id', 'azure_client_secret', "
                        "and 'azure_preferred_role' must be provided"
                    )

                def connect():
                    logger.debug("Connecting to redshift with Azure Credentials Provider...")
                    c = redshift_connector.connect(
                        iam=True,
                        region=self.credentials.region,
                        database=self.credentials.database,
                        cluster_identifier=self.credentials.cluster_id,
                        credentials_provider="AzureCredentialsProvider",
                        user=self.credentials.user,
                        password=self.credentials.password,
                        idp_tenant=self.credentials.azure_idp_tenant,
                        client_id=self.credentials.azure_client_id,
                        client_secret=self.credentials.azure_client_secret,
                        preferred_role=self.credentials.azure_preferred_role,
                    )
                    return c

            elif (self.credentials.credentials_provider.lower() == "oktacredentialsprovider") or (
                self.credentials.credentials_provider.lower() == "okta"
            ):
                if (
                    not self.credentials.okta_idp_host
                    or not self.credentials.okta_app_id
                    or not self.credentials.okta_app_name
                ):
                    raise dbt.exceptions.FailedToConnectError(
                        "Failed to use Okta credential. 'okta_idp_host', 'okta_app_id', 'okta_app_name' must be provided."
                    )

                def connect():
                    logger.debug("Connecting to redshift with Okta Credentials Provider...")
                    c = redshift_connector.connect(
                        iam=True,
                        region=self.credentials.region,
                        database=self.credentials.database,
                        cluster_identifier=self.credentials.cluster_id,
                        credentials_provider="OktaCredentialsProvider",
                        user=self.credentials.user,
                        password=self.credentials.password,
                        idp_host=self.credentials.okta_idp_host,
                        app_id=self.credentials.okta_app_id,
                        app_name=self.credentials.okta_app_name,
                    )
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
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
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
            raise dbt.exceptions.DbtRuntimeError(f"Tried to run invalid SQL: {sql} on {conn_name}")

        return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials
