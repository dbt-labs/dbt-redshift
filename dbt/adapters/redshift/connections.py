from multiprocessing import Lock
from contextlib import contextmanager
from typing import NewType

from dbt.adapters.postgres import PostgresConnectionManager
from dbt.adapters.postgres import PostgresCredentials
from dbt.events import AdapterLogger
import dbt.exceptions
import dbt.flags

import botocore
import boto3

from dbt.dataclass_schema import FieldEncoder, dbtClassMixin, StrEnum

from dataclasses import dataclass, field
from typing import Optional, List

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
class RedshiftCredentials(PostgresCredentials):
    method: str = RedshiftConnectionMethod.DATABASE  # type: ignore
    password: Optional[str] = None
    cluster_id: Optional[str] = field(
        default=None,
        metadata={
            "description": "If using IAM auth, the name of the cluster or serverless workgroup"
        },
    )
    iam_profile: Optional[str] = None
    iam_duration_seconds: int = 900
    search_path: Optional[str] = None
    keepalives_idle: int = 240
    autocreate: bool = False
    db_groups: List[str] = field(default_factory=list)
    ra3_node: Optional[bool] = False
    serverless: Optional[bool] = False

    @property
    def type(self):
        return "redshift"

    def _connection_keys(self):
        keys = super()._connection_keys()
        return keys + ("method", "cluster_id", "iam_profile", "iam_duration_seconds")


class RedshiftConnectionManager(PostgresConnectionManager):
    TYPE = "redshift"

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

    # TODO with the addition of the `serverless` field there are non non-nonsensical ways to call this function. It deserves a refactor.
    @classmethod
    def fetch_cluster_credentials(
        cls,
        db_user,
        db_name,
        cluster_id,
        iam_profile,
        duration_s,
        autocreate,
        db_groups,
        serverless,
    ):
        """Fetches temporary login credentials from AWS. The specified user
        must already exist in the database, or else an error will occur"""

        if iam_profile is None:
            session = boto3.Session()
        else:
            logger.debug(f"Connecting to Redshift using 'IAM' with profile {iam_profile}")
            session = boto3.Session(profile_name=iam_profile)

        try:
            if serverless:
                # redshift-serverless API is supported in boto3 1.24.20 and higher
                boto_client = session.client("redshift-serverless")
                return boto_client.get_credentials(
                    dbName=db_name, workgroupName=cluster_id, durationSeconds=duration_s
                )
            else:
                boto_client = session.client("redshift")
                return boto_client.get_cluster_credentials(
                    DbUser=db_user,
                    DbName=db_name,
                    ClusterIdentifier=cluster_id,
                    DurationSeconds=duration_s,
                    AutoCreate=autocreate,
                    DbGroups=db_groups,
                )

        except botocore.exceptions.ClientError as e:
            raise dbt.exceptions.FailedToConnectException(
                "Unable to get temporary Redshift cluster credentials: {}".format(e)
            )

    @classmethod
    def get_tmp_iam_cluster_credentials(cls, credentials):
        cluster_id = credentials.cluster_id

        # default via:
        # boto3.readthedocs.io/en/latest/reference/services/redshift.html
        iam_duration_s = credentials.iam_duration_seconds

        if not cluster_id:
            raise dbt.exceptions.FailedToConnectException(
                "'cluster_id' must be provided in profile if IAM " "authentication method selected"
            )

        cluster_creds = cls.fetch_cluster_credentials(
            credentials.user,
            credentials.database,
            credentials.cluster_id,
            credentials.iam_profile,
            iam_duration_s,
            credentials.autocreate,
            credentials.db_groups,
            credentials.serverless,
        )

        # the serverless API returns credentials in camel case
        if credentials.serverless:
            db_user = "dbUser"
            db_password = "dbPassword"
        else:
            db_user = "DbUser"
            db_password = "DbPassword"

        # replace username and password with temporary redshift credentials
        return credentials.replace(
            user=cluster_creds.get(db_user),
            password=cluster_creds.get(db_password),
        )

    @classmethod
    def get_credentials(cls, credentials):
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
            return credentials

        elif method == "iam":
            logger.debug("Connecting to Redshift using 'IAM' credentials")
            return cls.get_tmp_iam_cluster_credentials(credentials)

        else:
            raise dbt.exceptions.FailedToConnectException(
                "Invalid 'method' in profile: '{}'".format(method)
            )
