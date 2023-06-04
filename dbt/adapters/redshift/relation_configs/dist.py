from dataclasses import dataclass
from enum import Enum
from typing import Optional

from dbt.contracts.graph.model_config import NodeConfig
from dbt.exceptions import DbtRuntimeError


class DistStyle(str, Enum):
    auto = "auto"
    even = "even"
    all = "all"
    key = "key"

    @classmethod
    def default(cls) -> "DistStyle":
        return cls.auto


@dataclass(frozen=True, eq=True)
class DistConfig:
    dist_style: Optional[DistStyle] = DistStyle.default()
    dist_key: Optional[str] = None

    @classmethod
    def from_node_config(cls, node_config: NodeConfig) -> "DistConfig":
        dist = node_config.get("dist")
        if dist is None:
            dist_style = None
            dist_key = None
        elif dist.lower() in (DistStyle.auto, DistStyle.even, DistStyle.all):
            dist_style = dist
            dist_key = None
        else:
            dist_style = DistStyle.key
            dist_key = dist

        try:
            dist_config = cls._get_valid_dist_config(dist_style, dist_key)
        except DbtRuntimeError:
            raise DbtRuntimeError(f"Unexpected dist metadata retrieved from the config: {dist}")
        return dist_config

    @classmethod
    def from_database_config(cls, database_config: dict) -> "DistConfig":
        dist_style = database_config.get("diststyle")
        dist_key = database_config.get("column")

        try:
            dist_config = cls._get_valid_dist_config(dist_style, dist_key)
        except DbtRuntimeError:
            raise DbtRuntimeError(
                f"Unexpected dist metadata retrieved from the database: {database_config}"
            )
        return dist_config

    @classmethod
    def _get_valid_dist_config(
        cls, dist_style: Optional[str], dist_key: Optional[str]
    ) -> "DistConfig":
        if dist_style is None:
            dist_style_clean = None
        else:
            dist_style_clean = DistStyle(dist_style.lower())

        if dist_key is None:
            dist_key_clean = None
        else:
            dist_key_clean = dist_key.upper()  # TODO: do we need to look at the quoting policy?

        dist_config = DistConfig(dist_style=dist_style_clean, dist_key=dist_key_clean)
        if dist_config.is_valid:
            return dist_config
        raise DbtRuntimeError(
            f"Unexpected dist metadata: diststyle: {dist_style}; distkey: {dist_key}"
        )

    @property
    def is_valid(self) -> bool:
        if self.dist_style == DistStyle.key and self.dist_key is None:
            return False
        elif (
            self.dist_style in (DistStyle.auto, DistStyle.even, DistStyle.all)
            and self.dist_key is not None
        ):
            return False
        return True
