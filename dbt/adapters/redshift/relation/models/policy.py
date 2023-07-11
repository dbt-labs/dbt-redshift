from dataclasses import dataclass

from dbt.adapters.relation.models import IncludePolicy, QuotePolicy, RenderPolicy


MAX_CHARACTERS_IN_IDENTIFIER = 127


class RedshiftIncludePolicy(IncludePolicy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class RedshiftQuotePolicy(QuotePolicy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


RedshiftRenderPolicy = RenderPolicy(
    quote_policy=RedshiftQuotePolicy(),
    include_policy=RedshiftIncludePolicy(),
    quote_character='"',
    delimiter=".",
)
