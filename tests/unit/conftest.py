import agate
import pytest

from dbt.adapters.materialization import MaterializationFactory
from dbt.adapters.materialization.models import (
    MaterializationType,
    MaterializedViewMaterialization,
)
from dbt.adapters.relation import RelationFactory
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import DependsOn, ModelNode, NodeConfig
from dbt.contracts.relation import RelationType
from dbt.node_types import NodeType

from dbt.adapters.redshift.relation.models import (
    RedshiftMaterializedViewRelation,
    RedshiftMaterializedViewRelationChangeset,
    RedshiftRenderPolicy,
)


@pytest.fixture
def relation_factory():
    return RelationFactory(
        relation_types=RelationType,
        relation_models={
            RelationType.MaterializedView: RedshiftMaterializedViewRelation,
        },
        relation_changesets={
            RelationType.MaterializedView: RedshiftMaterializedViewRelationChangeset,
        },
        relation_can_be_renamed={
            RelationType.Table,
            RelationType.View,
        },
        render_policy=RedshiftRenderPolicy,
    )


@pytest.fixture
def materialization_factory(relation_factory):
    return MaterializationFactory(
        relation_factory=relation_factory,
        materialization_map={
            MaterializationType.MaterializedView: MaterializedViewMaterialization
        },
    )


@pytest.fixture
def materialized_view_ref(relation_factory):
    return relation_factory.make_ref(
        name="my_materialized_view",
        schema_name="my_schema",
        database_name="my_database",
        relation_type=RelationType.MaterializedView,
    )


@pytest.fixture
def view_ref(relation_factory):
    return relation_factory.make_ref(
        name="my_view",
        schema_name="my_schema",
        database_name="my_database",
        relation_type=RelationType.View,
    )


@pytest.fixture
def materialized_view_model_node():
    return ModelNode(
        alias="my_materialized_view",
        name="my_materialized_view",
        database="my_database",
        schema="my_schema",
        resource_type=NodeType.Model,
        unique_id="model.root.my_materialized_view",
        fqn=["root", "my_materialized_view"],
        package_name="root",
        original_file_path="my_materialized_view.sql",
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict(
            {
                "enabled": True,
                "materialized": "materialized_view",
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "vars": {},
                "quoting": {},
                "column_types": {},
                "tags": [],
                "autorefresh": True,
                "dist": "id",
                "sort": ["other_id"],
                "sort_type": "compound",
                "backup": False,
                "on_configuration_change": "continue",
            }
        ),
        tags=[],
        path="my_materialized_view.sql",
        language="sql",
        raw_code="select 42 from meaning_of_life",
        compiled_code="select 42 from meaning_of_life",
        description="",
        columns={},
        checksum=FileHash.from_contents(""),
    )


@pytest.fixture
def materialized_view_describe_relation_results():
    materialized_view_agate = agate.Table.from_object(
        [
            {
                "name": "my_materialized_view",
                "schema_name": "my_schema",
                "database_name": "my_database",
                "dist": """KEY("id")""",
                "sortkey": "other_id",
                "autorefresh": "t",
            }
        ]
    )

    query_agate = agate.Table.from_object(
        [
            {
                "query": "select 4 as id, 2 as other_id from meaning_of_life",
            }
        ]
    )

    return {"relation": materialized_view_agate, "query": query_agate}


@pytest.fixture
def materialized_view_relation(relation_factory, materialized_view_describe_relation_results):
    return relation_factory.make_from_describe_relation_results(
        materialized_view_describe_relation_results, RelationType.MaterializedView
    )


"""
Make sure the fixtures at least work, more thorough testing is done elsewhere
"""


def test_relation_factory(relation_factory):
    assert (
        relation_factory._get_relation_model(RelationType.MaterializedView)
        == RedshiftMaterializedViewRelation
    )


def test_materialization_factory(materialization_factory):
    redshift_parser = materialization_factory.relation_factory._get_relation_model(
        RelationType.MaterializedView
    )
    assert redshift_parser == RedshiftMaterializedViewRelation


def test_materialized_view_ref(materialized_view_ref):
    assert materialized_view_ref.name == "my_materialized_view"


def test_materialized_view_model_node(materialized_view_model_node):
    assert materialized_view_model_node.name == "my_materialized_view"


def test_materialized_view_relation(materialized_view_relation):
    assert materialized_view_relation.type == RelationType.MaterializedView
    assert materialized_view_relation.name == "my_materialized_view"
