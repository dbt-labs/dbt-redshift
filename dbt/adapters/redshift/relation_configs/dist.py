from dataclasses import dataclass
from typing import Optional, Set

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
            "distkey": config_dict.get("distkey"),
        }
        dist: "RedshiftDistConfig" = super().from_dict(kwargs_dict)  # type: ignore
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

        if dist == "":
            return {}
        elif dist.lower() in (
            RedshiftDistStyle.auto,
            RedshiftDistStyle.even,
            RedshiftDistStyle.all,
        ):
            return {"diststyle": dist.lower()}
        else:
            # TODO: include the QuotePolicy instead of defaulting to lower()
            return {"diststyle": RedshiftDistStyle.key, "distkey": dist.lower()}

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        """
        Translate agate objects from the database into a standard dictionary.

        Args:
            relation_results: the description of the distkey and diststyle from the database in this format:

                {
                    "dist": agate.Table(
                        agate.Row({
                            "dist": "<diststyle/distkey>",  # e.g. EVEN | KEY(column1) | AUTO(ALL) | AUTO(KEY(id))
                        })
                    )
                }

        Returns: a standard dictionary describing this `RedshiftDistConfig` instance
        """
        if dist := relation_results.get("dist"):
            dist = dist.rows[0].get("dist")
        else:
            return {}

        if dist[:3].lower() == "all":
            return {"diststyle": RedshiftDistStyle.all}

        elif dist[:4].lower() == "even":
            return {"diststyle": RedshiftDistStyle.even}

        elif dist[:4].lower() == "auto":
            return {"diststyle": RedshiftDistStyle.auto}

        elif dist[:3].lower() == "key":
            distkey = dist[len("key(") : -len(")")]
            return {"diststyle": RedshiftDistStyle.key, "distkey": distkey}

        return {}
