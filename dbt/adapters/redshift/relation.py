from dataclasses import dataclass
from typing import Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.context.providers import RuntimeConfigObject
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import RelationType
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation_configs import (
    RedshiftMaterializedViewConfig,
    RedshiftMaterializedViewConfigChangeset,
    RedshiftAutoRefreshConfigChange,
    RedshiftBackupConfigChange,
    RedshiftDistConfigChange,
    RedshiftSortConfigChange,
    RedshiftIncludePolicy,
    RedshiftQuotePolicy,
    MAX_CHARACTERS_IN_IDENTIFIER,
)


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(BaseRelation):
    include_policy = RedshiftIncludePolicy  # type: ignore
    quote_policy = RedshiftQuotePolicy  # type: ignore
    relation_configs = {
        RelationType.MaterializedView.value: RedshiftMaterializedViewConfig,
    }
    renameable_relations = frozenset(
        {
            RelationType.View,
            RelationType.Table,
        }
    )
    replaceable_relations = frozenset(
        {
            RelationType.View,
        }
    )

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

    @classmethod
    def from_runtime_config(cls, runtime_config: RuntimeConfigObject) -> RelationConfigBase:
        model_node: ModelNode = runtime_config.model
        relation_type: str = model_node.config.materialized

        if relation_config := cls.relation_configs.get(relation_type):
            return relation_config.from_model_node(model_node)

        raise DbtRuntimeError(
            f"from_runtime_config() is not supported for the provided relation type: {relation_type}"
        )

    @classmethod
    def materialized_view_config_changeset(
        cls, relation_results: RelationResults, runtime_config: RuntimeConfigObject
    ) -> Optional[RedshiftMaterializedViewConfigChangeset]:
        config_change_collection = RedshiftMaterializedViewConfigChangeset()

        existing_materialized_view = RedshiftMaterializedViewConfig.from_relation_results(
            relation_results
        )
        new_materialized_view = RedshiftMaterializedViewConfig.from_model_node(
            runtime_config.model
        )
        assert isinstance(existing_materialized_view, RedshiftMaterializedViewConfig)
        assert isinstance(new_materialized_view, RedshiftMaterializedViewConfig)

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
