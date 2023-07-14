from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, FrozenSet, Set

from dbt.adapters.relation.models import (
    DescribeRelationResults,
    RelationChange,
    RelationChangeAction,
    RelationComponent,
)
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.graph.nodes import ParsedNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models.policy import RedshiftRenderPolicy


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
class RedshiftSortRelation(RelationComponent, ValidationMixin):
    """
    This config fallows the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

    The following parameters are configurable by dbt:
    - sort_type: the type of sort key on the table/materialized view
        - defaults to `auto` if no sort config information is provided
        - defaults to `compound` if columns are provided, but type is omitted
    - sort_key: the column(s) to use for the sort key; cannot be combined with `sort_type=auto`
    """

    # attribution
    sortstyle: Optional[RedshiftSortStyle] = None
    sortkey: Optional[FrozenSet[str]] = field(default_factory=frozenset)  # type: ignore

    # configuration
    render = RedshiftRenderPolicy

    def __post_init__(self):
        # maintains `frozen=True` while allowing for a variable default on `sort_type`
        if self.sortstyle is None and self.sortkey == frozenset():
            object.__setattr__(self, "sortstyle", RedshiftSortStyle.default())
        elif self.sortstyle is None:
            object.__setattr__(self, "sortstyle", RedshiftSortStyle.default_with_columns())
        super().__post_init__()

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        # index rules get run by default with the mixin
        return {
            ValidationRule(
                validation_check=not (
                    self.sortstyle == RedshiftSortStyle.auto and self.sortkey != frozenset()
                ),
                validation_error=DbtRuntimeError(
                    "A `RedshiftSortConfig` that specifies a `sortkey` does not support the `sortstyle` of `auto`."
                ),
            ),
            ValidationRule(
                validation_check=not (
                    self.sortstyle in (RedshiftSortStyle.compound, RedshiftSortStyle.interleaved)
                    and self.sortkey == frozenset()
                ),
                validation_error=DbtRuntimeError(
                    "A `sortstyle` of `compound` or `interleaved` requires a `sortkey` to be provided."
                ),
            ),
            ValidationRule(
                validation_check=not (
                    self.sortstyle == RedshiftSortStyle.compound
                    and self.sortkey is not None
                    and len(self.sortkey) > 400
                ),
                validation_error=DbtRuntimeError(
                    "A compound `sortkey` only supports 400 columns."
                ),
            ),
            ValidationRule(
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
    def from_dict(cls, config_dict: Dict[str, Any]) -> "RedshiftSortRelation":
        # don't alter the incoming config
        kwargs_dict = deepcopy(config_dict)

        if sortstyle := config_dict.get("sortstyle"):
            kwargs_dict.update({"sortstyle": RedshiftSortStyle(sortstyle)})

        if sortkey := config_dict.get("sortkey"):
            kwargs_dict.update({"sortkey": frozenset(column for column in sortkey)})

        sort = super().from_dict(kwargs_dict)
        assert isinstance(sort, RedshiftSortRelation)
        return sort

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        """
        Translate ModelNode objects from the user-provided config into a standard dictionary.

        Args:
            node: the description of the sortkey and sortstyle from the user in this format:

                {
                    "sort_key": "<column_name>" or ["<column_name>"] or ["<column1_name>",...]
                    "sort_type": any("compound", "interleaved", "auto")
                }

        Returns: a standard dictionary describing this `RedshiftSortConfig` instance
        """
        config_dict = {}

        if sortstyle := node.config.extra.get("sort_type"):
            config_dict.update({"sortstyle": sortstyle.lower()})

        if sortkey := node.config.extra.get("sort"):
            # we allow users to specify the `sort_key` as a string if it's a single column
            if isinstance(sortkey, str):
                sortkey = [sortkey]

            config_dict.update({"sortkey": set(sortkey)})

        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        """
        Translate agate objects from the database into a standard dictionary.

        Note:
            This was only built for materialized views, which does not specify a sortstyle.
            Processing of `sortstyle` has been omitted here, which means it's the default (compound).

        Args:
            describe_relation_results: the description of the sortkey and sortstyle from the database in this format:

                agate.Row({
                    ...,
                    "sortkey1": "<column_name>",
                    ...
                })

        Returns: a standard dictionary describing this `RedshiftSortConfig` instance
        """
        describe_relation_results_entry = cls._parse_single_record_from_describe_relation_results(
            describe_relation_results, "sort"
        )
        if sortkey := describe_relation_results_entry.get("sortkey"):
            return {"sortkey": {sortkey}}
        return {}


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftSortRelationChange(RelationChange, ValidationMixin):
    context: RedshiftSortRelation

    @property
    def requires_full_refresh(self) -> bool:
        return True

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=(self.action == RelationChangeAction.alter),
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `alter` changes are supported for `sortkey` / `sortstyle`."
                ),
            ),
        }
