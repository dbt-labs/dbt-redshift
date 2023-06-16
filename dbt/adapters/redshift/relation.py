from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.context.providers import RuntimeConfigObject
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation_configs import (
    RedshiftMaterializedViewConfig,
)


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(BaseRelation):
    def __post_init__(self):
        # Check for length of Postgres table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > self.relation_max_name_length()
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {self.relation_max_name_length()} characters"
            )

    def relation_max_name_length(self):
        return 127

    def get_materialized_view_from_runtime_config(
        self, runtime_config: RuntimeConfigObject
    ) -> RedshiftMaterializedViewConfig:
        materialized_view = RedshiftMaterializedViewConfig.from_model_node(runtime_config.model)
        return materialized_view
