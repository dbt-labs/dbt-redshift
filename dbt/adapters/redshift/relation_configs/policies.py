from dataclasses import dataclass

from dbt.adapters.base.relation import Policy


class RedshiftIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class RedshiftQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True
