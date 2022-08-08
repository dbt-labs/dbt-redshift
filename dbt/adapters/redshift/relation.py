from dbt.adapters.base import Column
from dataclasses import dataclass
from dbt.adapters.postgres.relation import PostgresRelation


@dataclass(frozen=True, eq=False, repr=False)
class RedshiftRelation(PostgresRelation):
    # Override the method in the Postgres Relation because Redshift allows
    # longer names: "Be between 1 and 127 bytes in length, not including
    # quotation marks for delimited identifiers."
    #
    # see: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
    def relation_max_name_length(self):
        return 127


class RedshiftColumn(Column):
    pass  # redshift does not inherit from postgres here
