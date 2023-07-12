from typing import Type

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models import RedshiftDistRelation


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        ({"diststyle": "auto"}, None),
        ({"diststyle": "auto", "distkey": "id"}, DbtRuntimeError),
        ({"diststyle": "even"}, None),
        ({"diststyle": "even", "distkey": "id"}, DbtRuntimeError),
        ({"diststyle": "all"}, None),
        ({"diststyle": "all", "distkey": "id"}, DbtRuntimeError),
        ({"diststyle": "key"}, DbtRuntimeError),
        ({"diststyle": "key", "distkey": "id"}, None),
        ({}, None),
    ],
)
def test_create_index(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            RedshiftDistRelation.from_dict(config_dict)
    else:
        my_dist = RedshiftDistRelation.from_dict(config_dict)
        assert my_dist.diststyle == config_dict.get("diststyle", "auto")
        assert my_dist.distkey == config_dict.get("distkey")
