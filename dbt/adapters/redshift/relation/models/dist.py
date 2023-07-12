from copy import deepcopy
from dataclasses import dataclass
from typing import Optional, Set

import agate
from dbt.adapters.relation.models import (
    RelationChange,
    RelationChangeAction,
    RelationComponent,
)
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models.policy import RedshiftRenderPolicy


class RedshiftDistStyle(StrEnum):
    auto = "auto"
    even = "even"
    all = "all"
    key = "key"

    @classmethod
    def default(cls) -> "RedshiftDistStyle":
        return cls.auto


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftDistRelation(RelationComponent, ValidationMixin):
    """
    This config fallows the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

    The following parameters are configurable by dbt:
    - diststyle: the type of data distribution style to use on the table/materialized view
    - distkey: the column to use for the dist key if `dist_style` is `key`
    """

    # attribution
    diststyle: Optional[RedshiftDistStyle] = RedshiftDistStyle.default()
    distkey: Optional[str] = None

    # configuration
    render = RedshiftRenderPolicy

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        # index rules get run by default with the mixin
        return {
            ValidationRule(
                validation_check=not (
                    self.diststyle == RedshiftDistStyle.key and self.distkey is None
                ),
                validation_error=DbtRuntimeError(
                    "A `RedshiftDistConfig` that specifies a `diststyle` of `key` must provide a value for `distkey`."
                ),
            ),
            ValidationRule(
                validation_check=not (
                    self.diststyle
                    in (RedshiftDistStyle.auto, RedshiftDistStyle.even, RedshiftDistStyle.all)
                    and self.distkey is not None
                ),
                validation_error=DbtRuntimeError(
                    "A `RedshiftDistConfig` that specifies a `distkey` must be of `diststyle` `key`."
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "RedshiftDistRelation":
        # don't alter the incoming config
        kwargs_dict = deepcopy(config_dict)

        if diststyle := config_dict.get("diststyle"):
            kwargs_dict.update({"diststyle": RedshiftDistStyle(diststyle)})

        dist = super().from_dict(kwargs_dict)
        assert isinstance(dist, RedshiftDistRelation)
        return dist

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Translate ModelNode objects from the user-provided config into a standard dictionary.

        Args:
            model_node: the description of the distkey and diststyle from the user in this format:

                {
                    "dist": any("auto", "even", "all") or "<column_name>"
                }

        Returns: a standard dictionary describing this `RedshiftDistConfig` instance
        """
        dist = model_node.config.extra.get("dist", "")

        diststyle = dist.lower()

        if diststyle == "":
            config = {}

        elif diststyle in (
            RedshiftDistStyle.auto,
            RedshiftDistStyle.even,
            RedshiftDistStyle.all,
        ):
            config = {"diststyle": diststyle}

        else:
            config = {"diststyle": RedshiftDistStyle.key.value, "distkey": dist}

        return config

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            describe_relation_results: the description of the distkey and diststyle from the database in this format:

                agate.Row({
                    "diststyle": "<diststyle/distkey>",  # e.g. EVEN | KEY(column1) | AUTO(ALL) | AUTO(KEY(id))
                })

        Returns: a standard dictionary describing this `RedshiftDistConfig` instance
        """
        dist: str = describe_relation_results.get("dist")

        try:
            # covers `AUTO`, `ALL`, `EVEN`, `KEY`, '', <unexpected>
            diststyle = dist.split("(")[0].lower()
        except AttributeError:
            # covers None
            diststyle = ""

        if dist == "":
            config = {}

        elif diststyle == RedshiftDistStyle.key:
            open_paren = len("KEY(")
            close_paren = -len(")")
            distkey = dist[open_paren:close_paren]  # e.g. KEY(column1)
            config = {"diststyle": diststyle, "distkey": distkey}

        else:
            config = {"diststyle": diststyle}

        return config


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftDistRelationChange(RelationChange, ValidationMixin):
    context: RedshiftDistRelation

    @property
    def requires_full_refresh(self) -> bool:
        return True

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=(self.action == RelationChangeAction.alter),
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `alter` changes are supported for `distkey` / `diststyle`."
                ),
            ),
        }
