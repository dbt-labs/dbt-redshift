from typing import Type

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models import RedshiftSortRelation


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        ({}, None),
        ({"sortstyle": "auto", "sortkey": "id"}, DbtRuntimeError),
        ({"sortstyle": "compound", "sortkey": ["id"]}, None),
        ({"sortstyle": "interleaved", "sortkey": ["id"]}, None),
        ({"sortstyle": "auto"}, None),
        ({"sortstyle": "compound"}, DbtRuntimeError),
        ({"sortstyle": "interleaved"}, DbtRuntimeError),
        ({"sortstyle": "compound", "sortkey": ["id", "value"]}, None),
    ],
)
def test_create_sort(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            RedshiftSortRelation.from_dict(config_dict)
    else:
        my_sortkey = RedshiftSortRelation.from_dict(config_dict)
        default_sortstyle = "compound" if "sortkey" in config_dict else "auto"
        assert my_sortkey.sortstyle == config_dict.get("sortstyle", default_sortstyle)
        assert my_sortkey.sortkey == frozenset(config_dict.get("sortkey", {}))
