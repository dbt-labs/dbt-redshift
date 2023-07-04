from dataclasses import dataclass
from typing import Optional, FrozenSet, Set

import agate
from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation_configs.base import RedshiftRelationConfigBase


class RedshiftSortStyle(StrEnum):
    auto = "auto"
    compound = "compound"
    interleaved = "interleaved"

    @classmethod
    def default(cls) -> "RedshiftSortStyle":
        return cls.auto

    @classmethod
    def default_with_columns(cls) -> "RedshiftSortStyle":
        return cls.compound


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftSortConfig(RedshiftRelationConfigBase, RelationConfigValidationMixin):
    """
    This config fallows the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

    The following parameters are configurable by dbt:
    - sort_type: the type of sort key on the table/materialized view
        - defaults to `auto` if no sort config information is provided
        - defaults to `compound` if columns are provided, but type is omitted
    - sort_key: the column(s) to use for the sort key; cannot be combined with `sort_type=auto`
    """

    sortstyle: Optional[RedshiftSortStyle] = None
    sortkey: Optional[FrozenSet[str]] = None

    def __post_init__(self):
        # maintains `frozen=True` while allowing for a variable default on `sort_type`
        if self.sortstyle is None and self.sortkey is None:
            object.__setattr__(self, "sortstyle", RedshiftSortStyle.default())
        elif self.sortstyle is None:
            object.__setattr__(self, "sortstyle", RedshiftSortStyle.default_with_columns())
        super().__post_init__()

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # index rules get run by default with the mixin
        return {
            RelationConfigValidationRule(
                validation_check=not (
                    self.sortstyle == RedshiftSortStyle.auto and self.sortkey is not None
                ),
                validation_error=DbtRuntimeError(
                    "A `RedshiftSortConfig` that specifies a `sortkey` does not support the `sortstyle` of `auto`."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.sortstyle in (RedshiftSortStyle.compound, RedshiftSortStyle.interleaved)
                    and self.sortkey is None
                ),
                validation_error=DbtRuntimeError(
                    "A `sortstyle` of `compound` or `interleaved` requires a `sortkey` to be provided."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.sortstyle == RedshiftSortStyle.compound
                    and self.sortkey is not None
                    and len(self.sortkey) > 400
                ),
                validation_error=DbtRuntimeError(
                    "A compound `sortkey` only supports 400 columns."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.sortstyle == RedshiftSortStyle.interleaved
                    and self.sortkey is not None
                    and len(self.sortkey) > 8
                ),
                validation_error=DbtRuntimeError(
                    "An interleaved `sortkey` only supports 8 columns."
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "RedshiftSortConfig":
        kwargs_dict = {
            "sortstyle": config_dict.get("sortstyle"),
            "sortkey": frozenset(column for column in config_dict.get("sortkey", {})),
        }
        sort: "RedshiftSortConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return sort

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Translate ModelNode objects from the user-provided config into a standard dictionary.

        Args:
            model_node: the description of the sortkey and sortstyle from the user in this format:

                {
                    "sort_key": "<column_name>" or ["<column_name>"] or ["<column1_name>",...]
                    "sort_type": any("compound", "interleaved", "auto")
                }

        Returns: a standard dictionary describing this `RedshiftSortConfig` instance
        """
        config_dict = {}

        if sortstyle := model_node.config.extra.get("sort_type"):
            config_dict.update({"sortstyle": sortstyle.lower()})

        if sortkey := model_node.config.extra.get("sort"):
            # we allow users to specify the `sort_key` as a string if it's a single column
            if isinstance(sortkey, str):
                sortkey = [sortkey]

            config_dict.update({"sortkey": set(sortkey)})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Note:
            This was only built for materialized views, which does not specify a sortstyle.
            Processing of `sortstyle` has been omitted here, which means it's the default (compound).

        Args:
            relation_results_entry: the description of the sortkey and sortstyle from the database in this format:

                agate.Row({
                    ...,
                    "sortkey1": "<column_name>",
                    ...
                })

        Returns: a standard dictionary describing this `RedshiftSortConfig` instance
        """
        if sortkey := relation_results_entry.get("sortkey1"):
            return {"sortkey": {sortkey}}
        return {}


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftSortConfigChange(RelationConfigChange, RelationConfigValidationMixin):
    context: RedshiftSortConfig

    @property
    def requires_full_refresh(self) -> bool:
        return True

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=(self.action == RelationConfigChangeAction.alter),
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `alter` changes are supported for `sortkey` / `sortstyle`."
                ),
            ),
        }
