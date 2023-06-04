from dataclasses import dataclass
from typing import Optional

from dbt.contracts.graph.model_config import NodeConfig
from dbt.exceptions import DbtRuntimeError

from dist import DistConfig, DistStyle
from sort import SortConfig


@dataclass(eq=True)
class MaterializedViewConfig:
    """
    `name` and `query` are required attributes for a materialized view in Redshift.
    However, they are marked as optional here because they are not technically part of the config.
    Also, there are common situations where these are not easily available, and requiring them adds little
    to no functionality.

    - name: see above
    - query: see above
    - dist: The default `diststyle` for materialized views is EVEN, despite the default in general being AUTO.
    """

    name: Optional[str] = None  # see docstring above
    query: Optional[str] = None  # see docstring above
    backup: Optional[bool] = True
    dist: Optional[DistConfig] = DistConfig(dist_style=DistStyle.even)
    sort: Optional[SortConfig] = SortConfig()
    auto_refresh: Optional[bool] = False

    @classmethod
    def from_node_config(cls, node_config: NodeConfig) -> "MaterializedViewConfig":
        # `name` and `query` are not part of the config
        kwargs = {}

        backup = node_config.get("backup")
        if backup:
            kwargs.update({"backup": backup})

        dist = node_config.get("dist")
        if dist:
            kwargs.update({"dist": DistConfig.from_node_config(node_config)})

        sort = node_config.get("sort")
        if sort:
            kwargs.update({"sort": SortConfig.from_node_config(node_config)})

        auto_refresh = node_config.get("auto_refresh")
        if auto_refresh:
            kwargs.update({"auto_refresh": auto_refresh})

        materialized_view = MaterializedViewConfig(**kwargs)
        if materialized_view.is_valid:
            return materialized_view
        raise DbtRuntimeError(f"Unexpected dist metadata retrieved from the config: {node_config}")

    @classmethod
    def from_database_config(cls, database_config: dict) -> "MaterializedViewConfig":
        kwargs = {}

        name = database_config.get("name")
        if name:
            kwargs.update({"name": name})

        query = database_config.get("query")
        if query:
            kwargs.update({"query": query})

        backup = database_config.get("backup")
        if backup:
            kwargs.update({"backup": backup})

        dist = database_config.get("dist")
        if dist:
            kwargs.update({"dist": DistConfig.from_database_config(database_config)})

        sort = database_config.get("sort")
        if sort:
            kwargs.update({"sort": SortConfig.from_database_config(database_config)})

        auto_refresh = database_config.get("auto_refresh")
        if auto_refresh:
            kwargs.update({"auto_refresh": auto_refresh})

        materialized_view = MaterializedViewConfig(**kwargs)
        if materialized_view.is_valid:
            return materialized_view
        raise DbtRuntimeError(
            f"Unexpected dist metadata retrieved from the config: {database_config}"
        )

    @property
    def is_valid(self) -> bool:
        if self.dist and not self.dist.is_valid:
            return False
        elif self.sort and not self.sort.is_valid:
            return False
        return True
