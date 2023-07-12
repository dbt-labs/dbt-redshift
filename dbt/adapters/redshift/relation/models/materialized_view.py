from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, Optional, Set

import agate
from dbt.adapters.relation.models import (
    MaterializedViewRelation,
    MaterializedViewRelationChangeset,
    Relation,
    RelationChange,
    RelationChangeAction,
)
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models.dist import (
    RedshiftDistRelation,
    RedshiftDistRelationChange,
    RedshiftDistStyle,
)
from dbt.adapters.redshift.relation.models.policy import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    RedshiftRenderPolicy,
)
from dbt.adapters.redshift.relation.models.schema import RedshiftSchemaRelation
from dbt.adapters.redshift.relation.models.sort import (
    RedshiftSortRelation,
    RedshiftSortRelationChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftMaterializedViewRelation(MaterializedViewRelation, ValidationMixin):
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

    # attribution
    name: str
    schema: RedshiftSchemaRelation
    query: str
    backup: Optional[bool] = True
    dist: RedshiftDistRelation = RedshiftDistRelation.from_dict({"diststyle": "even"})
    sort: RedshiftSortRelation = RedshiftSortRelation.from_dict({})
    autorefresh: Optional[bool] = False

    # configuration
    render = RedshiftRenderPolicy
    SchemaParser = RedshiftSchemaRelation  # type: ignore
    can_be_renamed = False

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        # sort and dist rules get run by default with the mixin
        return {
            ValidationRule(
                validation_check=len(self.name or "") <= MAX_CHARACTERS_IN_IDENTIFIER,
                validation_error=DbtRuntimeError(
                    f"The materialized view name is more than {MAX_CHARACTERS_IN_IDENTIFIER} "
                    f"characters: {self.name}"
                ),
            ),
            ValidationRule(
                validation_check=self.dist.diststyle != RedshiftDistStyle.auto,
                validation_error=DbtRuntimeError(
                    "Redshift materialized views do not support a `diststyle` of `auto`."
                ),
            ),
            ValidationRule(
                validation_check=len(self.name if self.name else "") <= 127,
                validation_error=DbtRuntimeError(
                    "Redshift does not support object names longer than 127 characters."
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "RedshiftMaterializedViewRelation":
        # don't alter the incoming config
        kwargs_dict = deepcopy(config_dict)

        # this preserves the materialized view-specific default of `even` over the general default of `auto`
        if dist := config_dict.get("dist"):
            kwargs_dict.update({"dist": RedshiftDistRelation.from_dict(dist)})

        if sort := config_dict.get("sort"):
            kwargs_dict.update({"sort": RedshiftSortRelation.from_dict(sort)})

        materialized_view = super().from_dict(kwargs_dict)
        assert isinstance(materialized_view, RedshiftMaterializedViewRelation)
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = super().parse_model_node(model_node)

        config_dict.update(
            {
                "backup": model_node.config.extra.get("backup"),
                "autorefresh": model_node.config.extra.get("autorefresh"),
            }
        )

        if model_node.config.get("dist"):
            config_dict.update({"dist": RedshiftDistRelation.parse_model_node(model_node)})

        if model_node.config.get("sort"):
            config_dict.update({"sort": RedshiftSortRelation.parse_model_node(model_node)})

        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            describe_relation_results: the description of the materialized view from the database in this format:

                {
                    "materialized_view": agate.Table(
                        agate.Row({
                            "database_name": "<database_name>",
                            "schema_name": "<schema_name>",
                            "name": "<name>",
                            "dist": "<diststyle/distkey>",  # e.g. EVEN | KEY(column1) | AUTO(ALL) | AUTO(KEY(id)),
                            "sortkey": "<column_name>",
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
        # merge these because the base class assumes `query` is on the same record as `name`, `schema_name` and
        # `database_name`
        describe_relation_results = cls._combine_describe_relation_results_tables(
            describe_relation_results
        )
        config_dict = super().parse_describe_relation_results(describe_relation_results)

        materialized_view: agate.Row = describe_relation_results["materialized_view"].rows[0]
        config_dict.update(
            {
                "autorefresh": materialized_view.get("autorefresh"),
                "query": cls._parse_query(materialized_view.get("query")),
            }
        )

        # the default for materialized views differs from the default for diststyle in general
        # only set it if we got a value
        if materialized_view.get("dist"):
            config_dict.update(
                {"dist": RedshiftDistRelation.parse_describe_relation_results(materialized_view)}
            )

        # TODO: this only shows the first column in the sort key
        if materialized_view.get("sortkey"):
            config_dict.update(
                {"sort": RedshiftSortRelation.parse_describe_relation_results(materialized_view)}
            )

        return config_dict

    @classmethod
    def _combine_describe_relation_results_tables(
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> Dict[str, agate.Table]:
        materialized_view_table: agate.Table = describe_relation_results["materialized_view"]
        query_table: agate.Table = describe_relation_results["query"]
        combined_table: agate.Table = materialized_view_table.join(query_table, full_outer=True)
        return {"materialized_view": combined_table}

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
        return query
        # open_paren = query.find("as (")
        # close_paren = query.find(");")
        # return query[open_paren:close_paren].strip()


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftAutoRefreshRelationChange(RelationChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftBackupRelationChange(RelationChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass
class RedshiftMaterializedViewRelationChangeset(MaterializedViewRelationChangeset):
    backup: Optional[RedshiftBackupRelationChange] = None
    dist: Optional[RedshiftDistRelationChange] = None
    sort: Optional[RedshiftSortRelationChange] = None
    autorefresh: Optional[RedshiftAutoRefreshRelationChange] = None

    @classmethod
    def parse_relations(cls, existing_relation: Relation, target_relation: Relation) -> dict:
        try:
            assert isinstance(existing_relation, RedshiftMaterializedViewRelation)
            assert isinstance(target_relation, RedshiftMaterializedViewRelation)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two Redshift materialized view relations were expected, but received:\n"
                f"    existing: {existing_relation}\n"
                f"    new: {target_relation}\n"
            )

        config_dict = super().parse_relations(existing_relation, target_relation)

        if target_relation.autorefresh != existing_relation.autorefresh:
            config_dict.update(
                {
                    "autorefresh": RedshiftAutoRefreshRelationChange(
                        action=RelationChangeAction.alter,
                        context=target_relation.autorefresh,
                    )
                }
            )

        if target_relation.backup != existing_relation.backup:
            config_dict.update(
                {
                    "backup": RedshiftBackupRelationChange(
                        action=RelationChangeAction.alter,
                        context=target_relation.backup,
                    )
                }
            )

        if target_relation.dist != existing_relation.dist:
            config_dict.update(
                {
                    "dist": RedshiftDistRelationChange(
                        action=RelationChangeAction.alter,
                        context=target_relation.dist,
                    )
                }
            )

        if target_relation.sort != existing_relation.sort:
            config_dict.update(
                {
                    "sort": RedshiftSortRelationChange(
                        action=RelationChangeAction.alter,
                        context=target_relation.sort,
                    )
                }
            )

        return config_dict

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
    def is_empty(self) -> bool:
        return not any({self.backup, self.dist, self.sort, self.autorefresh}) and super().is_empty
