from dataclasses import dataclass
from typing import Optional

from dbt.adapters.base.relation import Policy
from dbt.contracts.relation import ComponentName


MAX_CHARACTERS_IN_IDENTIFIER = 127


class RedshiftIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class RedshiftQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


def render_part(component: ComponentName, value: Optional[str]) -> Optional[str]:
    include_policy = RedshiftIncludePolicy()
    quote_policy = RedshiftQuotePolicy()
    if include_policy.get_part(component) and value:
        if quote_policy.get_part(component):
            return f'"{value}"'
        return value.lower()
    return None
