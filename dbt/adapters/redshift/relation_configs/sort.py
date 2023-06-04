from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union, List, Set

from dbt.contracts.graph.model_config import NodeConfig
from dbt.exceptions import DbtRuntimeError


class SortType(str, Enum):
    auto = "auto"
    compound = "compound"
    interleaved = "interleaved"

    @classmethod
    def default(cls) -> "SortType":
        return cls.auto

    @classmethod
    def default_with_columns(cls) -> "SortType":
        return cls.compound


@dataclass(eq=True)
class SortConfig:
    sort_type: Optional[
        SortType
    ] = None  # the default value changes based on `sort_key`, see `__post_init__`
    sort_key: Optional[Set[str]] = None

    def __post_init__(self):
        if self.sort_type is None and self.sort_key is None:
            self.sort_type = SortType.default()
        elif self.sort_type is None:
            self.sort_type = SortType.default_with_columns()

    @classmethod
    def from_node_config(cls, node_config: NodeConfig) -> "SortConfig":
        sort_type = node_config.get("sort_type")
        sort_key: Union[list, str] = node_config.get("sort")
        if isinstance(sort_key, str):
            sort_key = [sort_key]

        try:
            sort_config = cls._get_valid_sort_config(sort_type, sort_key)
        except DbtRuntimeError:
            raise DbtRuntimeError(
                f"Unexpected sort metadata retrieved from the config: {sort_type}"
            )
        return sort_config

    @classmethod
    def from_database_config(cls, database_config: dict) -> "SortConfig":
        sort_type = database_config.get("type")
        sort_key = database_config.get("columns")

        try:
            sort_config = cls._get_valid_sort_config(sort_type, sort_key)
        except DbtRuntimeError:
            raise DbtRuntimeError(
                f"Unexpected sort metadata retrieved from the database: {database_config}"
            )
        return sort_config

    @classmethod
    def _get_valid_sort_config(
        cls, sort_type: Optional[str], sort_key: Optional[List[str]]
    ) -> "SortConfig":
        if sort_type is None:
            sort_type_clean = None
        else:
            sort_type_clean = SortType(sort_type.lower())

        if sort_key is None:
            sort_key_clean = None
        else:
            sort_key_clean = set(
                column.upper() for column in sort_key
            )  # TODO: do we need to look at the quoting policy?

        sort_config = SortConfig(sort_type=sort_type_clean, sort_key=sort_key_clean)
        if sort_config.is_valid:
            return sort_config
        raise DbtRuntimeError(f"Unexpected sort metadata: type: {sort_type}; sortkey:{sort_key}")

    @property
    def is_valid(self) -> bool:
        if self.sort_type == SortType.auto and self.sort_key is not None:
            return False
        elif (
            self.sort_type == SortType.compound
            and self.sort_key is not None
            and len(self.sort_key) > 400
        ):
            return False
        elif (
            self.sort_type == SortType.interleaved
            and self.sort_key is not None
            and len(self.sort_key) > 8
        ):
            return False
        elif self.sort_type in (SortType.compound, SortType.interleaved) and self.sort_key is None:
            return False
        return True
