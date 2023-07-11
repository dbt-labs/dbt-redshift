from typing import Type

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models import RedshiftIndexRelation


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        ({"column_names": frozenset({"id", "value"}), "method": "hash", "unique": False}, None),
        ({"column_names": frozenset("id"), "method": "btree", "unique": True}, None),
        ({}, DbtRuntimeError),
        ({"method": "btree", "unique": True}, DbtRuntimeError),
    ],
)
# TODO replace this with sort/dist stuff
def test_create_index(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            RedshiftIndexRelation.from_dict(config_dict)
    else:
        my_index = RedshiftIndexRelation.from_dict(config_dict)
        assert my_index.column_names == config_dict.get("column_names")
