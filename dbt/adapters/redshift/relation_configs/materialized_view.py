from dataclasses import dataclass
from typing import Optional, Set

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.redshift.relation_configs.dist import RedshiftDistConfig, RedshiftDistStyle
from dbt.adapters.redshift.relation_configs.sort import RedshiftSortConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftMaterializedViewConfig(RelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-create-sql-command.html

    The following parameters are configurable by dbt:
    - mv_name: name of the materialized view
    - query: the query that defines the view
    - backup: determines if the materialized view is included in automated and manual cluster snapshots
    - dist: the distribution configuration for the data behind the materialized view, a combination of
    a `diststyle` and an optional `distkey`
        - Note: the default `diststyle` for materialized views is EVEN, despite the default in general being AUTO
    - sort: the sort configuration for the data behind the materialized view, a combination of
    a `sortstyle` and an optional `sortkey`
    - auto_refresh: specifies whether the materialized view should be automatically refreshed
    with latest changes from its base tables

    There are currently no non-configurable parameters.
    """

    mv_name: Optional[str] = None  # see docstring above
    query: Optional[str] = None  # see docstring above
    backup: Optional[bool] = True
    dist: Optional[RedshiftDistConfig] = RedshiftDistConfig(diststyle=RedshiftDistStyle.even)
    sort: Optional[RedshiftSortConfig] = RedshiftSortConfig()
    auto_refresh: Optional[bool] = False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # sort and dist rules get run by default with the mixin
        return set()

    @classmethod
    def from_dict(cls, config_dict) -> "RedshiftMaterializedViewConfig":
        kwargs_dict = {
            "mv_name": config_dict.get("mv_name"),
            "query": config_dict.get("query"),
            "backup": config_dict.get("backup"),
            "dist": RedshiftDistConfig.from_dict(config_dict.get("dist")),
            "sort": RedshiftSortConfig.from_dict(config_dict.get("sort")),
            "auto_refresh": config_dict.get("auto_refresh"),
        }
        materialized_view: "RedshiftMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> "RedshiftMaterializedViewConfig":
        materialized_view_config = cls.parse_model_node(model_node)
        materialized_view = cls.from_dict(materialized_view_config)
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "mv_name": model_node.identifier,
            "query": model_node.compiled_code,
            "backup": model_node.config.get("backup"),
            "auto_refresh": model_node.config.get("auto_refresh"),
        }

        if dist := model_node.config.get("dist"):
            config_dict.update({"dist": RedshiftDistConfig.parse_model_node(dist)})

        if sort := model_node.config.get("sort"):
            config_dict.update({"sort": RedshiftSortConfig.parse_model_node(sort)})

        return config_dict

    @classmethod
    def from_relation_results(
        cls, relation_results: RelationResults
    ) -> "RedshiftMaterializedViewConfig":
        materialized_view_config = cls.parse_relation_results(relation_results)
        materialized_view = cls.from_dict(materialized_view_config)
        return materialized_view

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        base_config = relation_results.get("base", {})

        config_dict = {
            "mv_name": base_config.get("mv_name"),
            "query": base_config.get("query"),
            "backup": base_config.get("backup"),
            "dist": RedshiftDistConfig.parse_relation_results(relation_results),
            "sort": RedshiftDistConfig.parse_relation_results(relation_results),
            "auto_refresh": base_config.get("auto_refresh"),
        }

        return config_dict
