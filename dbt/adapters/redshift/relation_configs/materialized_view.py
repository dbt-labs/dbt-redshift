from dataclasses import dataclass
from typing import Optional, Set

import agate
from dbt.adapters.relation_configs import (
    RelationResults,
    RelationConfigChange,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation_configs.base import RedshiftRelationConfigBase
from dbt.adapters.redshift.relation_configs.dist import (
    RedshiftDistConfig,
    RedshiftDistStyle,
    RedshiftDistConfigChange,
)
from dbt.adapters.redshift.relation_configs.policies import MAX_CHARACTERS_IN_IDENTIFIER
from dbt.adapters.redshift.relation_configs.sort import (
    RedshiftSortConfig,
    RedshiftSortConfigChange,
)
from dbt.adapters.redshift.utility import evaluate_bool


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftMaterializedViewConfig(RedshiftRelationConfigBase, RelationConfigValidationMixin):
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

    mv_name: str
    schema_name: str
    database_name: str
    query: str
    backup: bool = True
    dist: RedshiftDistConfig = RedshiftDistConfig(diststyle=RedshiftDistStyle.even)
    sort: RedshiftSortConfig = RedshiftSortConfig()
    autorefresh: bool = False

    @property
    def path(self) -> str:
        return ".".join(
            part
            for part in [self.database_name, self.schema_name, self.mv_name]
            if part is not None
        )

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # sort and dist rules get run by default with the mixin
        return {
            RelationConfigValidationRule(
                validation_check=len(self.mv_name or "") <= MAX_CHARACTERS_IN_IDENTIFIER,
                validation_error=DbtRuntimeError(
                    f"The materialized view name is more than {MAX_CHARACTERS_IN_IDENTIFIER} "
                    f"characters: {self.mv_name}"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=self.dist.diststyle != RedshiftDistStyle.auto,
                validation_error=DbtRuntimeError(
                    "Redshift materialized views do not support a `diststyle` of `auto`."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=len(self.mv_name if self.mv_name else "") <= 127,
                validation_error=DbtRuntimeError(
                    "Redshift does not support object names longer than 127 characters."
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "RedshiftMaterializedViewConfig":
        kwargs_dict = {
            "mv_name": cls._render_part(ComponentName.Identifier, config_dict.get("mv_name")),
            "schema_name": cls._render_part(ComponentName.Schema, config_dict.get("schema_name")),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict.get("database_name")
            ),
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
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "mv_name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
        }

        # backup/autorefresh can be bools or strings
        backup_value = model_node.config.extra.get("backup")
        if backup_value is not None:
            config_dict["backup"] = evaluate_bool(backup_value)

        autorefresh_value = model_node.config.extra.get("auto_refresh")
        if autorefresh_value is not None:
            config_dict["autorefresh"] = evaluate_bool(autorefresh_value)

        if query := model_node.compiled_code:
            config_dict.update({"query": query.strip()})

        if model_node.config.get("dist"):
            config_dict.update({"dist": RedshiftDistConfig.parse_model_node(model_node)})

        if model_node.config.get("sort"):
            config_dict.update({"sort": RedshiftSortConfig.parse_model_node(model_node)})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            relation_results: the description of the materialized view from the database in this format:

                {
                    "materialized_view": agate.Table(
                        agate.Row({
                            "database": "<database_name>",
                            "schema": "<schema_name>",
                            "table": "<name>",
                            "diststyle": "<diststyle/distkey>",  # e.g. EVEN | KEY(column1) | AUTO(ALL) | AUTO(KEY(id)),
                            "sortkey1": "<column_name>",
                            "autorefresh: any("t", "f"),
                        })
                    ),
                    "query": agate.Table(
                        agate.Row({"definition": "<query>")}
                    ),
                }

                Additional columns in either value is fine, as long as `sortkey` and `sortstyle` are available.

        Returns: a standard dictionary describing this `RedshiftMaterializedViewConfig` instance
        """
        materialized_view: agate.Row = cls._get_first_row(
            relation_results.get("materialized_view")
        )
        query: agate.Row = cls._get_first_row(relation_results.get("query"))

        config_dict = {
            "mv_name": materialized_view.get("table"),
            "schema_name": materialized_view.get("schema"),
            "database_name": materialized_view.get("database"),
            "query": cls._parse_query(query.get("definition")),
        }

        autorefresh_value = materialized_view.get("autorefresh")
        if autorefresh_value is not None:
            bool_filter = {"t": True, "f": False}
            config_dict["autorefresh"] = bool_filter.get(autorefresh_value, autorefresh_value)

        # the default for materialized views differs from the default for diststyle in general
        # only set it if we got a value
        if materialized_view.get("diststyle"):
            config_dict.update(
                {"dist": RedshiftDistConfig.parse_relation_results(materialized_view)}
            )

        # TODO: this only shows the first column in the sort key
        if materialized_view.get("sortkey1"):
            config_dict.update(
                {"sort": RedshiftSortConfig.parse_relation_results(materialized_view)}
            )

        return config_dict

    @classmethod
    def _parse_query(cls, query: str) -> str:
        """
        Get the select statement from the materialized view definition in Redshift.

        Args:
            query: the `create materialized view` statement from `pg_views`, for example:

            create materialized view my_materialized_view
                backup yes
                diststyle even
                sortkey (id)
                auto refresh no
            as (
                select * from my_base_table
            );

        Returns: the `select ...` statement, for example:

            select * from my_base_table

        """
        open_paren = query.find("as (") + len("as (")
        close_paren = query.find(");")
        return query[open_paren:close_paren].strip()


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftAutoRefreshConfigChange(RelationConfigChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftBackupConfigChange(RelationConfigChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass
class RedshiftMaterializedViewConfigChangeset:
    backup: Optional[RedshiftBackupConfigChange] = None
    dist: Optional[RedshiftDistConfigChange] = None
    sort: Optional[RedshiftSortConfigChange] = None
    autorefresh: Optional[RedshiftAutoRefreshConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.autorefresh.requires_full_refresh if self.autorefresh else False,
                self.backup.requires_full_refresh if self.backup else False,
                self.dist.requires_full_refresh if self.dist else False,
                self.sort.requires_full_refresh if self.sort else False,
            }
        )

    @property
    def has_changes(self) -> bool:
        return any(
            {
                self.backup if self.backup else False,
                self.dist if self.dist else False,
                self.sort if self.sort else False,
                self.autorefresh if self.autorefresh else False,
            }
        )
