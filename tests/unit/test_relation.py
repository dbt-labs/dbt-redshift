from unittest.mock import Mock

import agate
import pytest

from dbt.adapters.redshift.relation import RedshiftRelation
from dbt.adapters.contracts.relation import (
    RelationType,
    RelationConfig,
)

from dbt.adapters.redshift.relation_configs.sort import RedshiftSortStyle


def test_renameable_relation():
    relation = RedshiftRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_table",
        type=RelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
        {
            RelationType.View,
            RelationType.Table,
        }
    )


@pytest.fixture
def materialized_view_without_sort_key_from_db():
    materialized_view = agate.Table.from_object(
        [
            {
                "database": "my_db",
                "schema": "my_schema",
                "table": "my_table",
            }
        ],
    )

    column_descriptor = agate.Table.from_object([])

    query = agate.Table.from_object(
        [
            {
                "definition": "create materialized view my_view as (select 1 as my_column, 'value' as my_column2)"
            }
        ]
    )

    relation_results = {
        "materialized_view": materialized_view,
        "column_descriptor": column_descriptor,
        "query": query,
    }
    return relation_results


@pytest.fixture
def materialized_view_without_sort_key_config():
    relation_config = Mock(spec=RelationConfig)

    relation_config.database = "my_db"
    relation_config.identifier = "my_table"
    relation_config.schema = "my_schema"
    relation_config.config = Mock()
    relation_config.config.extra = {}
    relation_config.config.sort = ""
    relation_config.compiled_code = "create materialized view my_view as (select 1 as my_column, 'value' as my_column2)"
    return relation_config


@pytest.fixture
def materialized_view_multiple_sort_key_from_db(materialized_view_without_sort_key_from_db):
    materialized_view_without_sort_key_from_db["column_descriptor"] = agate.Table.from_object(
        [
            {
                "schema": "my_schema",
                "table": "my_table",
                "column": "my_column",
                "is_dist_key": True,
                "is_sort_key": True,
            },
            {
                "schema": "my_schema",
                "table": "my_table",
                "column": "my_column2",
                "is_dist_key": True,
                "is_sort_key": True,
            },
        ],
    )
    return materialized_view_without_sort_key_from_db


@pytest.fixture
def materialized_view_multiple_sort_key_config(materialized_view_without_sort_key_config):
    materialized_view_without_sort_key_config.config.extra = {
        "sort_type": RedshiftSortStyle.compound,
        "sort": ["my_column", "my_column2"],
    }

    return materialized_view_without_sort_key_config

def test_materialized_view_config_changeset_without_sort_key_empty_changes(
    materialized_view_without_sort_key_from_db,
    materialized_view_without_sort_key_config,
):
    change_set = RedshiftRelation.materialized_view_config_changeset(
        materialized_view_without_sort_key_from_db,
        materialized_view_without_sort_key_config,
    )

    assert change_set is None

def test_materialized_view_config_changeset_multiple_sort_key_without_changes(
    materialized_view_multiple_sort_key_from_db,
    materialized_view_multiple_sort_key_config,
):

    change_set = RedshiftRelation.materialized_view_config_changeset(
        materialized_view_multiple_sort_key_from_db,
        materialized_view_multiple_sort_key_config,
    )

    assert change_set is None


def test_materialized_view_config_changeset_multiple_sort_key_with_changes(
    materialized_view_multiple_sort_key_from_db,
    materialized_view_multiple_sort_key_config,
):
    materialized_view_multiple_sort_key_config.config.extra["sort"].append("my_column3")

    change_set = RedshiftRelation.materialized_view_config_changeset(
        materialized_view_multiple_sort_key_from_db,
        materialized_view_multiple_sort_key_config,
    )

    assert change_set is not None
    assert change_set.sort.context.sortkey == frozenset({"my_column", "my_column2", "my_column3"})
