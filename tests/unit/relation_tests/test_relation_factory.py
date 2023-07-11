"""
Uses the following fixtures in `unit/dbt_redshift_tests/conftest.py`:
- `relation_factory`
- `materialized_view_stub`
"""

from dbt.contracts.relation import RelationType

from dbt.adapters.postgres.relation import models


def test_make_stub(materialized_view_stub):
    assert materialized_view_stub.name == "my_materialized_view"
    assert materialized_view_stub.schema_name == "my_schema"
    assert materialized_view_stub.database_name == "my_database"
    assert materialized_view_stub.type == "materialized_view"
    assert materialized_view_stub.can_be_renamed is True


def test_make_backup_stub(relation_factory, materialized_view_stub):
    backup_stub = relation_factory.make_backup_stub(materialized_view_stub)
    assert backup_stub.name == '"my_materialized_view__dbt_backup"'


def test_make_intermediate(relation_factory, materialized_view_stub):
    intermediate_relation = relation_factory.make_intermediate(materialized_view_stub)
    assert intermediate_relation.name == '"my_materialized_view__dbt_tmp"'


def test_make_from_describe_relation_results(
    relation_factory, materialized_view_describe_relation_results
):
    materialized_view = relation_factory.make_from_describe_relation_results(
        materialized_view_describe_relation_results, RelationType.MaterializedView
    )

    assert materialized_view.name == "my_materialized_view"
    assert materialized_view.schema_name == "my_schema"
    assert materialized_view.database_name == "my_database"
    assert materialized_view.query == "select 42 from meaning_of_life"

    index_1 = models.RedshiftIndexRelation(
        column_names=frozenset({"id", "value"}),
        method=models.RedshiftIndexMethod.hash,
        unique=False,
        render=models.RedshiftRenderPolicy,
    )
    index_2 = models.RedshiftIndexRelation(
        column_names=frozenset({"id"}),
        method=models.RedshiftIndexMethod.btree,
        unique=True,
        render=models.RedshiftRenderPolicy,
    )
    assert index_1 in materialized_view.indexes
    assert index_2 in materialized_view.indexes


def test_make_from_model_node(relation_factory, materialized_view_model_node):
    materialized_view = relation_factory.make_from_model_node(materialized_view_model_node)

    assert materialized_view.name == "my_materialized_view"
    assert materialized_view.schema_name == "my_schema"
    assert materialized_view.database_name == "my_database"
    assert materialized_view.query == "select 42 from meaning_of_life"

    index_1 = models.RedshiftIndexRelation(
        column_names=frozenset({"id", "value"}),
        method=models.RedshiftIndexMethod.hash,
        unique=False,
        render=models.RedshiftRenderPolicy,
    )
    index_2 = models.RedshiftIndexRelation(
        column_names=frozenset({"id"}),
        method=models.RedshiftIndexMethod.btree,
        unique=True,
        render=models.RedshiftRenderPolicy,
    )
    assert index_1 in materialized_view.indexes
    assert index_2 in materialized_view.indexes
