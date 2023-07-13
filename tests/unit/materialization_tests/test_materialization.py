from dataclasses import replace

from dbt.adapters.materialization.models import (
    MaterializedViewMaterialization,
    MaterializationBuildStrategy,
)

from dbt.adapters.redshift.relation import models


def test_materialized_view_create(materialized_view_model_node, relation_factory):
    materialization = MaterializedViewMaterialization.from_node(
        materialized_view_model_node, relation_factory
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Create
    assert materialization.should_revoke_grants is False


def test_materialized_view_replace(materialized_view_model_node, relation_factory, view_ref):
    materialization = MaterializedViewMaterialization.from_node(
        materialized_view_model_node, relation_factory, view_ref
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Replace
    assert materialization.should_revoke_grants is True


def test_materialized_view_alter(
    materialized_view_model_node, relation_factory, materialized_view_relation
):
    altered_materialized_view = replace(
        materialized_view_relation, sort=models.RedshiftSortRelation.from_dict({})
    )

    materialization = MaterializedViewMaterialization.from_node(
        materialized_view_model_node, relation_factory, altered_materialized_view
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Alter
    assert materialization.should_revoke_grants is True
