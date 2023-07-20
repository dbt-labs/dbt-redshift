from dataclasses import dataclass
from typing import Any, Dict, Optional

import agate
from dbt.adapters.base.relation import Policy
from dbt.adapters.relation.models import RelationComponent, DescribeRelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.redshift.relation.models._policy import (
    RedshiftIncludePolicy,
    RedshiftQuotePolicy,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftRelationComponent(RelationComponent):
    """
    This base class implements a few boilerplate methods and provides some light structure for Redshift relations.
    """

    @classmethod
    def include_policy(cls) -> Policy:
        return RedshiftIncludePolicy()

    @classmethod
    def quote_policy(cls) -> Policy:
        return RedshiftQuotePolicy()

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "RedshiftRelationComponent":
        relation = super().from_dict(config_dict)
        assert isinstance(relation, RedshiftRelationComponent)
        return relation

    @classmethod
    def from_node(cls, node: ModelNode) -> "RedshiftRelationComponent":
        relation_config = cls.parse_node(node)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    def parse_node(cls, node: ModelNode) -> Dict[str, Any]:
        raise NotImplementedError(f"`parse_node()` needs to be implemented on {cls.__name__}")

    @classmethod
    def from_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> "RedshiftRelationComponent":
        relation_config = cls.parse_describe_relation_results(describe_relation_results)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        raise NotImplementedError(
            f"`parse_describe_relation_results()` needs to be implemented on {cls.__name__}"
        )

    @classmethod
    def _render_part(cls, component: ComponentName, value: Optional[str]) -> Optional[str]:
        if cls.include_policy().get_part(component) and value:
            if cls.quote_policy().get_part(component):
                return f'"{value}"'
            return value.lower()
        return None

    @classmethod
    def _get_first_row(cls, results: agate.Table) -> agate.Row:
        try:
            return results.rows[0]
        except IndexError:
            return agate.Row(values=set())
