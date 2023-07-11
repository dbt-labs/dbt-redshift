from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models import MAX_CHARACTERS_IN_IDENTIFIER


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(BaseRelation):
    def __post_init__(self):
        # Check for length of Redshift table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > MAX_CHARACTERS_IN_IDENTIFIER
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {MAX_CHARACTERS_IN_IDENTIFIER} characters"
            )

    def relation_max_name_length(self):
        return MAX_CHARACTERS_IN_IDENTIFIER
