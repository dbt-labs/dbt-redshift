from dataclasses import dataclass
from typing import Optional

from dbt.adapters.postgres.relation import PostgresRelation
from dbt.adapters.relation_configs import (
    RelationConfigChangeAction,
    RelationResults,
)
from dbt.context.providers import RuntimeConfigObject

from dbt.adapters.redshift.relation_configs import (
    RedshiftMaterializedViewConfig,
    RedshiftMaterializedViewConfigChangeCollection,
    RedshiftAutoRefreshConfigChange,
    RedshiftBackupConfigChange,
    RedshiftDistConfigChange,
    RedshiftSortConfigChange,
)


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(PostgresRelation):
    # Override the method in the Postgres Relation because Redshift allows
    # longer names: "Be between 1 and 127 bytes in length, not including
    # quotation marks for delimited identifiers."
    #
    # see: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    def relation_max_name_length(self):
        return 127

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

        if new_materialized_view.auto_refresh != existing_materialized_view.auto_refresh:
            config_change_collection.auto_refresh = RedshiftAutoRefreshConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.auto_refresh,
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
