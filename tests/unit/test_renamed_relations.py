from dbt.contracts.relation import RelationType

from dbt.adapters.redshift.relation import RedshiftRelation


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
