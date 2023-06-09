from dataclasses import dataclass
from typing import Optional, Set

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation_configs.dist import (
    RedshiftDistConfig,
    RedshiftDistStyle,
)
from dbt.adapters.redshift.relation_configs.sort import (
    RedshiftSortConfig,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftMaterializedViewConfig(RelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-create-sql-command.html

    The following parameters are configurable by dbt:
    - mv_name: name of the materialized view
    - query: the query that defines the view
    - backup: determines if the materialized view is included in automated and manual cluster snapshots
        - Note: we cannot currently query this from Redshift, which creates two issues
            - a model deployed with this set to False will rebuild every run because the database version will always
            look like True
            - to deploy this as a change from False to True, a full refresh must be issued since the database version
            will always look like True (unless there is another full refresh-triggering change)
    - dist: the distribution configuration for the data behind the materialized view, a combination of
    a `diststyle` and an optional `distkey`
        - Note: the default `diststyle` for materialized views is EVEN, despite the default in general being AUTO
    - sort: the sort configuration for the data behind the materialized view, a combination of
    a `sortstyle` and an optional `sortkey`
    - auto_refresh: specifies whether the materialized view should be automatically refreshed
    with latest changes from its base tables

    There are currently no non-configurable parameters.
    """

    mv_name: Optional[str] = None
    query: Optional[str] = None
    backup: bool = True
    dist: RedshiftDistConfig = RedshiftDistConfig(diststyle=RedshiftDistStyle.even)
    sort: RedshiftSortConfig = RedshiftSortConfig()
    autorefresh: bool = False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # sort and dist rules get run by default with the mixin
        return {
            RelationConfigValidationRule(
                validation_check=(self.dist.diststyle != RedshiftDistStyle.auto),
                validation_error=DbtRuntimeError(
                    "Redshift materialized views do not support a `diststyle` of `auto`."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=self.mv_name is None or len(self.mv_name) <= 127,
                validation_error=DbtRuntimeError(
                    "Redshift does not support object names longer than 127 characters."
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "RedshiftMaterializedViewConfig":
        kwargs_dict = {
            "mv_name": config_dict.get("mv_name"),
            "query": config_dict.get("query"),
            "backup": config_dict.get("backup"),
            "autorefresh": config_dict.get("autorefresh"),
        }

        # this preserves the materialized view-specific default of `even` over the general default of `auto`
        if dist := config_dict.get("dist"):
            kwargs_dict.update({"dist": RedshiftDistConfig.from_dict(dist)})

        if sort := config_dict.get("sort"):
            kwargs_dict.update({"sort": RedshiftSortConfig.from_dict(sort)})

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
            "mv_name": model_node.relation_name,
            "backup": model_node.config.get("backup"),
            "autorefresh": model_node.config.get("auto_refresh"),
        }

        if query := model_node.compiled_code:
            config_dict.update({"query": query.strip()})

        if model_node.config.get("dist"):
            config_dict.update({"dist": RedshiftDistConfig.parse_model_node(model_node)})

        if model_node.config.get("sort"):
            config_dict.update({"sort": RedshiftSortConfig.parse_model_node(model_node)})

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
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            relation_results: the description of the materialized view from the database in this format:

                {
                    "materialized_view": agate.Table(
                        agate.Row({
                            "mv_name": "<name>",
                            "dist": "<diststyle/distkey>",  # e.g. EVEN | KEY(column1) | AUTO(ALL) | AUTO(KEY(id))
                            "autorefresh: any("t", "f"),
                            "backup": "<backup>",  # currently not able to be retrieved
                        })
                    ),
                    "query": agate.Table(
                        agate.Row({"query": "<query>")}
                    ),
                    "sortkey": agate.Table(
                        [
                            agate.Row({"sortkey": "<column_name>"}),
                            ...multiple, one per column in the sortkey
                        ]
                    )
                }

                Additional columns in either value is fine, as long as `sortkey` and `sortstyle` are available.

        Returns: a standard dictionary describing this `RedshiftMaterializedViewConfig` instance
        """
        if materialized_view := relation_results.get("materialized_view"):
            materialized_view_config = materialized_view.rows[0]
        else:
            materialized_view_config = {}

        config_dict = {
            "mv_name": materialized_view_config.get("mv_name"),
            "dist": RedshiftDistConfig.parse_relation_results({"dist": materialized_view}),
        }

        if autorefresh := materialized_view_config.get("autorefresh"):
            config_dict.update({"autorefresh": {"t": True, "f": False}.get(autorefresh)})

        if query_config := relation_results.get("query"):
            query_config = query_config.rows[0]
        else:
            query_config = {}

        if query := query_config.get("query"):
            config_dict.update({"query": query})

        if sortkey := relation_results.get("sortkey"):
            if sort := RedshiftSortConfig.parse_relation_results({"sortkey": sortkey}):
                config_dict.update({"sort": sort})

        return config_dict
