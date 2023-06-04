from dataclasses import dataclass
from typing import Optional

from dbt.adapters.postgres.relation import PostgresRelation
from dbt.contracts.graph.model_config import NodeConfig
import agate

from dbt.adapters.redshift.relation_configs import MaterializedViewConfig


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(PostgresRelation):
    # Override the method in the Postgres Relation because Redshift allows
    # longer names: "Be between 1 and 127 bytes in length, not including
    # quotation marks for delimited identifiers."
    #
    # see: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    def relation_max_name_length(self):
        return 127

    def get_updates(self, database_config: agate.Row, node_config: NodeConfig) -> Optional[dict]:
        if self.is_materialized_view:
            return self._get_materialized_view_updates(dict(database_config), node_config)
        return None

    def _get_materialized_view_updates(
        self, database_config: dict, node_config: NodeConfig
    ) -> Optional[dict]:
        existing_materialized_view = MaterializedViewConfig.from_database_config(
            dict(database_config)
        )
        new_materialized_view = MaterializedViewConfig.from_node_config(node_config)

        updates = {}
        if new_materialized_view.dist != existing_materialized_view.dist:
            updates.update({"dist": new_materialized_view.dist})

        if new_materialized_view.sort != existing_materialized_view.sort:
            updates.update({"sort": new_materialized_view.sort})

        if new_materialized_view.auto_refresh != existing_materialized_view.auto_refresh:
            updates.update({"auto_refresh": new_materialized_view.auto_refresh})

        if updates:
            return updates
        return None
