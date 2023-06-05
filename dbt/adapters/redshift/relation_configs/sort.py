from dataclasses import dataclass
from typing import Optional, FrozenSet, Set

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError


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
class RedshiftSortConfig(RelationConfigBase, RelationConfigValidationMixin):
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
        config_dict = {}

        if sortstyle := model_node.config.get("sort_type"):
            config_dict.update({"sortstyle": sortstyle.lower()})

        if sortkey := model_node.config.get("sort_key"):
            # we allow users to specify the `sort_key` as a string if it's a single column
            if isinstance(sortkey, str):
                sortkey = [sortkey]

            # TODO: include the QuotePolicy instead of defaulting to lower()
            config_dict.update({"sortkey": set(column.lower() for column in sortkey)})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        sort = relation_results.get("base", {})

        config_dict = {
            "sortstyle": sort.get("sortstyle"),
        }

        if column_names := sort.get("sortkey"):
            # we shouldn't have to adjust the values from the database for the QuotePolicy
            config_dict.update(
                {"sortkey": set(column.lower() for column in column_names.split(","))}
            )

        return config_dict


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
