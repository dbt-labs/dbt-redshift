"""
Uses the following fixtures in `unit/dbt_redshift_tests/conftest.py`:
- `relation_factory`
- `materialized_view_ref`
"""
import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.redshift.relation import models


def test_make_ref(materialized_view_ref):
    assert materialized_view_ref.name == "my_materialized_view"
    assert materialized_view_ref.schema_name == "my_schema"
    assert materialized_view_ref.database_name == "my_database"
    assert materialized_view_ref.type == "materialized_view"
    assert materialized_view_ref.can_be_renamed is False


def test_make_backup_ref(relation_factory, materialized_view_ref):
    # materialized views cannot be renamed in redshift
    with pytest.raises(DbtRuntimeError):
        relation_factory.make_backup_ref(materialized_view_ref)


def test_make_intermediate(relation_factory, materialized_view_ref):
    # materialized views cannot be renamed in redshift
    with pytest.raises(DbtRuntimeError):
        relation_factory.make_intermediate(materialized_view_ref)


def test_make_from_describe_relation_results(relation_factory, materialized_view_relation):
    assert materialized_view_relation.name == "my_materialized_view"
    assert materialized_view_relation.schema_name == "my_schema"
    assert materialized_view_relation.database_name == "my_database"
    assert materialized_view_relation.query == "select 4 as id, 2 as other_id from meaning_of_life"
    sort = models.RedshiftSortRelation(
        sortstyle=models.RedshiftSortStyle.compound,
        sortkey=frozenset({"other_id"}),
        render=models.RedshiftRenderPolicy,
    )
    assert materialized_view_relation.sort == sort
    dist = models.RedshiftDistRelation(
        diststyle=models.RedshiftDistStyle.key,
        distkey='"id"',
        render=models.RedshiftRenderPolicy,
    )
    assert materialized_view_relation.dist == dist
    assert materialized_view_relation.autorefresh is True


def test_make_from_model_node(relation_factory, materialized_view_model_node):
    materialized_view = relation_factory.make_from_model_node(materialized_view_model_node)

    assert materialized_view.name == "my_materialized_view"
    assert materialized_view.schema_name == "my_schema"
    assert materialized_view.database_name == "my_database"
    assert materialized_view.query == "select 42 from meaning_of_life"
    sort = models.RedshiftSortRelation(
        sortstyle=models.RedshiftSortStyle.compound,
        sortkey=frozenset({"other_id"}),
        render=models.RedshiftRenderPolicy,
    )
    assert materialized_view.sort == sort
    dist = models.RedshiftDistRelation(
        diststyle=models.RedshiftDistStyle.key,
        distkey="id",
        render=models.RedshiftRenderPolicy,
    )
    assert materialized_view.dist == dist
    assert materialized_view.autorefresh is True
