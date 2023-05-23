from dataclasses import dataclass
from typing import Optional
from dbt.adapters.postgres.relation import PostgresRelation
from dbt.contracts.graph.model_config import NodeConfig


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(PostgresRelation):
    # Override the method in the Postgres Relation because Redshift allows
    # longer names: "Be between 1 and 127 bytes in length, not including
    # quotation marks for delimited identifiers."
    #
    # see: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    def relation_max_name_length(self):
        return 127

    def get_dist_updates(self, dist: str, config: NodeConfig) -> Optional[str]:
        new_dist = config.get("dist", "")
        if dist != new_dist:
            dist = new_dist
            return dist
        else:
            return dist
