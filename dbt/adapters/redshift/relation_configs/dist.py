from dataclasses import dataclass
from typing import Optional, Set, Dict

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError


class RedshiftDistStyle(StrEnum):
    auto = "auto"
    even = "even"
    all = "all"
    key = "key"

    @classmethod
    def default(cls) -> "RedshiftDistStyle":
        return cls.auto


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RedshiftDistConfig(RelationConfigBase, RelationConfigValidationMixin):
    """
    This config fallows the specs found here:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

    The following parameters are configurable by dbt:
    - diststyle: the type of data distribution style to use on the table/materialized view
    - distkey: the column to use for the dist key if `dist_style` is `key`
    """

    diststyle: Optional[RedshiftDistStyle] = RedshiftDistStyle.default()
    distkey: Optional[str] = None

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # index rules get run by default with the mixin
        return {
            RelationConfigValidationRule(
                validation_check=not (
                    self.diststyle == RedshiftDistStyle.key and self.distkey is None
                ),
                validation_error=DbtRuntimeError(
                    "A `RedshiftDistConfig` that specifies a `diststyle` of `key` must provide a value for `distkey`."
                ),
            ),
            RelationConfigValidationRule(
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
    def from_dict(cls, config_dict) -> "RedshiftDistConfig":
        kwargs_dict = {
            "diststyle": config_dict.get("diststyle"),
            "distkey": frozenset(column for column in config_dict.get("distkey", {})),
        }
        dist: "RedshiftDistConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return dist

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        dist = model_node.config.get("dist")

        config_dict: Dict[str, Optional[str]] = {}

        if dist is None:
            config_dict.update({"diststyle": None, "distkey": None})
        elif dist.lower() in (
            RedshiftDistStyle.auto,
            RedshiftDistStyle.even,
            RedshiftDistStyle.all,
        ):
            # TODO: include the QuotePolicy instead of defaulting to lower()
            config_dict.update({"diststyle": dist.lower(), "distkey": None})
        else:
            # TODO: include the QuotePolicy instead of defaulting to lower()
            config_dict.update({"diststyle": RedshiftDistStyle.key, "distkey": dist.lower()})
        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        dist = relation_results.get("base", {})
        config_dict = {
            "diststyle": dist.get("diststyle"),
            "distkey": dist.get("distkey"),
        }
        return config_dict
