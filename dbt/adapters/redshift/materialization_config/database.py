from dbt.adapters.materialization_config import (
    DatabaseConfig,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)

from dbt.adapters.redshift.materialization_config.policy import redshift_conform_part


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftDatabaseConfig(DatabaseConfig):
    """
    This config follow the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_DATABASE.html

    The following parameters are configurable by dbt:
    - name: name of the database
    """

    name: str

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-redshift requires a name for a database, received: {self.name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "RedshiftDatabaseConfig":
        """
        Because this returns a frozen dataclass, this method should be overridden if additional parameters are supplied.
        """
        kwargs_dict = {"name": redshift_conform_part(ComponentName.Database, config_dict["name"])}
        database = super().from_dict(kwargs_dict)
        assert isinstance(database, RedshiftDatabaseConfig)
        return database

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Because this returns a `dict`, this method should be extended if additional parameters are supplied.
        """
        config_dict = {"name": model_node.database}
        super().parse_model_node()
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        """
        Because this returns a `dict`, this method should be extended if additional parameters are supplied.
        """
        config_dict = {"name": describe_relation_results["database"]}
        return config_dict
