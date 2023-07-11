from typing import Type

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models import RedshiftDatabaseRelation


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        ({"name": "my_database"}, None),
        ({"name": ""}, DbtRuntimeError),
        ({"wrong_name": "my_database"}, DbtRuntimeError),
        ({}, DbtRuntimeError),
    ],
)
def test_make_database(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            RedshiftDatabaseRelation.from_dict(config_dict)
    else:
        my_database = RedshiftDatabaseRelation.from_dict(config_dict)
        assert my_database.name == config_dict.get("name")
