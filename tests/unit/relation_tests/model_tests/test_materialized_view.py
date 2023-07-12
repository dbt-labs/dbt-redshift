from dataclasses import replace
from typing import Type

import pytest

from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation.models import (
    RedshiftDistRelation,
    RedshiftMaterializedViewRelation,
    RedshiftMaterializedViewRelationChangeset,
    RedshiftSortRelation,
)


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        (
            {
                "name": "my_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 1 from my_favoriate_table",
            },
            None,
        ),
        (
            {
                "name": "my_indexed_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 42 from meaning_of_life",
                "dist": {"diststyle": "key", "distkey": "id"},
                "sort": {"sortstyle": "compound", "sortkey": ["id", "value"]},
                "autorefresh": True,
            },
            None,
        ),
        (
            {
                "my_name": "my_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                # missing "query"
            },
            DbtRuntimeError,
        ),
        (
            {
                "name": "my_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 1 from my_favoriate_table",
                "dist": {"diststyle": "auto"},  # "auto" not supported for Redshift MVs
            },
            DbtRuntimeError,
        ),
        (
            {
                "name": "my_super_long_named_materialized_view"
                * 10,  # names must be <= 127 characters
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 1 from my_favoriate_table",
            },
            DbtRuntimeError,
        ),
    ],
)
def test_create_materialized_view(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            RedshiftMaterializedViewRelation.from_dict(config_dict)
    else:
        my_materialized_view = RedshiftMaterializedViewRelation.from_dict(config_dict)

        assert my_materialized_view.name == config_dict.get("name")
        assert my_materialized_view.schema_name == config_dict.get("schema", {}).get("name")
        assert my_materialized_view.database_name == config_dict.get("schema", {}).get(
            "database", {}
        ).get("name")
        assert my_materialized_view.query == config_dict.get("query")
        assert my_materialized_view.backup == config_dict.get("backup", True)

        default_dist = RedshiftDistRelation.from_dict({"diststyle": "even"})
        default_diststyle = default_dist.diststyle
        default_distkey = default_dist.distkey
        assert my_materialized_view.dist.diststyle == config_dict.get("dist", {}).get(
            "diststyle", default_diststyle
        )
        assert my_materialized_view.dist.distkey == config_dict.get("dist", {}).get(
            "distkey", default_distkey
        )

        default_sort = RedshiftSortRelation.from_dict({})
        default_sortstyle = default_sort.sortstyle
        default_sortkey = default_sort.sortkey
        assert my_materialized_view.sort.sortstyle == config_dict.get("sort", {}).get(
            "sortstyle", default_sortstyle
        )
        assert my_materialized_view.sort.sortkey == frozenset(
            config_dict.get("sort", {}).get("sortkey", default_sortkey)
        )

        assert my_materialized_view.autorefresh == config_dict.get("autorefresh", False)
        assert my_materialized_view.can_be_renamed is False


@pytest.mark.parametrize(
    "changes,is_empty,requires_full_refresh",
    [
        ({"autorefresh": "f"}, False, False),
        ({"sort": RedshiftSortRelation.from_dict({"sortkey": "id"})}, False, True),
        ({}, True, False),
    ],
)
def test_create_materialized_view_changeset(
    materialized_view_relation, changes, is_empty, requires_full_refresh
):
    existing_materialized_view = replace(materialized_view_relation)
    target_materialized_view = replace(existing_materialized_view, **changes)

    changeset = RedshiftMaterializedViewRelationChangeset.from_relations(
        existing_materialized_view, target_materialized_view
    )
    assert changeset.is_empty is is_empty
    assert changeset.requires_full_refresh is requires_full_refresh
