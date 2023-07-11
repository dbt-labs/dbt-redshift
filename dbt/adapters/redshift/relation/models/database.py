from dataclasses import dataclass
from typing import Set

from dbt.adapters.relation.models import DatabaseRelation
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models.policy import RedshiftRenderPolicy


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftDatabaseRelation(DatabaseRelation, ValidationMixin):
    """
    This config follow the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_DATABASE.html

    The following parameters are configurable by dbt:
    - name: name of the database
    """

    # attribution
    name: str

    # configuration
    render = RedshiftRenderPolicy

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-redshift requires a name for a database, received: {self.name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict) -> "RedshiftDatabaseRelation":
        database = super().from_dict(config_dict)
        assert isinstance(database, RedshiftDatabaseRelation)
        return database
