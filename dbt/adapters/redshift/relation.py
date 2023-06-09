from dataclasses import dataclass
from typing import Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import (
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.context.providers import RuntimeConfigObject
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation_configs import (
    RedshiftMaterializedViewConfig,
    RedshiftMaterializedViewConfigChangeCollection,
    RedshiftAutoRefreshConfigChange,
    RedshiftBackupConfigChange,
    RedshiftDistConfigChange,
    RedshiftSortConfigChange,
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

    def get_materialized_view_config_change_collection(  # type: ignore
        self, relation_results: RelationResults, runtime_config: RuntimeConfigObject
    ) -> Optional[RedshiftMaterializedViewConfigChangeCollection]:
        config_change_collection = RedshiftMaterializedViewConfigChangeCollection()

        existing_materialized_view = RedshiftMaterializedViewConfig.from_relation_results(
            relation_results
        )
        new_materialized_view = RedshiftMaterializedViewConfig.from_model_node(
            runtime_config.model
        )

        if new_materialized_view.autorefresh != existing_materialized_view.autorefresh:
            config_change_collection.autorefresh = RedshiftAutoRefreshConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.autorefresh,
            )

        if new_materialized_view.backup != existing_materialized_view.backup:
            config_change_collection.backup = RedshiftBackupConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.backup,
            )

        if new_materialized_view.dist != existing_materialized_view.dist:
            config_change_collection.dist = RedshiftDistConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.dist,
            )

        if new_materialized_view.sort != existing_materialized_view.sort:
            config_change_collection.sort = RedshiftSortConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.sort,
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None
