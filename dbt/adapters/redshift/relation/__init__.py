from dataclasses import dataclass
from typing import Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation.models import (
    DescribeRelationResults,
    RelationChangeAction,
    RelationComponent,
)
from dbt.context.providers import RuntimeConfigObject
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import RelationType
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    RedshiftAutoRefreshRelationChange,
    RedshiftBackupRelationChange,
    RedshiftDistRelationChange,
    RedshiftIncludePolicy,
    RedshiftMaterializedViewRelation,
    RedshiftMaterializedViewRelationChangeset,
    RedshiftQuotePolicy,
    RedshiftSortRelationChange,
)


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(BaseRelation):
    include_policy = RedshiftIncludePolicy  # type: ignore
    quote_policy = RedshiftQuotePolicy  # type: ignore
    relation_configs = {
        RelationType.MaterializedView.value: RedshiftMaterializedViewRelation,
    }

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
    def from_runtime_config(cls, runtime_config: RuntimeConfigObject) -> RelationComponent:
        model_node: ModelNode = runtime_config.model
        relation_type: str = model_node.config.materialized

        if relation_config := cls.relation_configs.get(relation_type):
            return relation_config.from_node(model_node)

        raise DbtRuntimeError(
            f"from_runtime_config() is not supported for the provided relation type: {relation_type}"
        )

    @classmethod
    def materialized_view_config_changeset(
        cls,
        describe_relation_results: DescribeRelationResults,
        runtime_config: RuntimeConfigObject,
    ) -> Optional[RedshiftMaterializedViewRelationChangeset]:
        config_change_collection = RedshiftMaterializedViewRelationChangeset()

        existing_materialized_view = (
            RedshiftMaterializedViewRelation.from_describe_relation_results(
                describe_relation_results
            )
        )
        new_materialized_view = RedshiftMaterializedViewRelation.from_node(runtime_config.model)
        assert isinstance(existing_materialized_view, RedshiftMaterializedViewRelation)
        assert isinstance(new_materialized_view, RedshiftMaterializedViewRelation)

        if new_materialized_view.autorefresh != existing_materialized_view.autorefresh:
            config_change_collection.autorefresh = RedshiftAutoRefreshRelationChange(
                action=RelationChangeAction.alter,
                context=new_materialized_view.autorefresh,
            )

        if new_materialized_view.backup != existing_materialized_view.backup:
            config_change_collection.backup = RedshiftBackupRelationChange(
                action=RelationChangeAction.alter,
                context=new_materialized_view.backup,
            )

        if new_materialized_view.dist != existing_materialized_view.dist:
            config_change_collection.dist = RedshiftDistRelationChange(
                action=RelationChangeAction.alter,
                context=new_materialized_view.dist,
            )

        if new_materialized_view.sort != existing_materialized_view.sort:
            config_change_collection.sort = RedshiftSortRelationChange(
                action=RelationChangeAction.alter,
                context=new_materialized_view.sort,
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None
