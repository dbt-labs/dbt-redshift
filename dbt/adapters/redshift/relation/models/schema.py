from dataclasses import dataclass
from typing import Set

from dbt.adapters.relation.models import SchemaRelation
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models.database import RedshiftDatabaseRelation
from dbt.adapters.redshift.relation.models.policy import RedshiftRenderPolicy


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftSchemaRelation(SchemaRelation, ValidationMixin):
    """
    This config follow the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_SCHEMA.html

    The following parameters are configurable by dbt:
    - name: name of the schema
    - database_name: name of the database
    """

    # attribution
    name: str

    # configuration
    render = RedshiftRenderPolicy
    DatabaseParser = RedshiftDatabaseRelation

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        """
        Returns: a set of rules that should evaluate to `True` (i.e. False == validation failure)
        """
        return {
            ValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-redshift requires a name to reference a schema, received:\n"
                    f"    schema: {self.name}\n"
                ),
            ),
        }
