from dataclasses import replace

from dbt.adapters.materialization.models import (
    MaterializedViewMaterialization,
    MaterializationBuildStrategy,
)


def test_materialized_view_create(materialized_view_runtime_config, relation_factory):
    materialization = MaterializedViewMaterialization.from_runtime_config(
        materialized_view_runtime_config, relation_factory
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Create
    assert materialization.should_revoke_grants is False


def test_materialized_view_replace(materialized_view_runtime_config, relation_factory, view_stub):
    materialization = MaterializedViewMaterialization.from_runtime_config(
        materialized_view_runtime_config, relation_factory, view_stub
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Replace
    assert materialization.should_revoke_grants is True


def test_materialized_view_alter(
    materialized_view_runtime_config, relation_factory, materialized_view_relation
):
    altered_materialized_view = replace(materialized_view_relation, indexes={})

    materialization = MaterializedViewMaterialization.from_runtime_config(
        materialized_view_runtime_config, relation_factory, altered_materialized_view
    )
    assert materialization.build_strategy == MaterializationBuildStrategy.Alter
    assert materialization.should_revoke_grants is True
