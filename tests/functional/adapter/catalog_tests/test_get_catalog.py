from dbt.contracts.relation import RelationType
from dbt.tests.util import get_connection, get_manifest, run_dbt
import pytest

from tests.functional.adapter.catalog_tests import files


class TestGetCatalog:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": files.MY_TABLE,
            "my_view.sql": files.MY_VIEW,
            "my_materialized_view.sql": files.MY_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    @pytest.fixture(scope="class")
    def my_schema(self, project, adapter):
        yield adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="",
        )

    @pytest.fixture(scope="class")
    def my_seed(self, adapter, my_schema):
        yield adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_seed",
            type=RelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_table(self, adapter, my_schema, my_seed):
        yield adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_table",
            type=RelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_view(self, adapter, my_schema, my_seed):
        yield adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_view",
            type=RelationType.View,
        )

    @pytest.fixture(scope="class")
    def my_materialized_view(self, adapter, my_schema, my_seed):
        yield adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="my_materialized_view",
            type=RelationType.MaterializedView,
        )

    @pytest.fixture(scope="class")
    def my_information_schema(self, adapter, my_schema):
        yield adapter.Relation.create(
            database=my_schema.database,
            schema=my_schema.schema,
            identifier="INFORMATION_SCHEMA",
        ).information_schema()

    @pytest.fixture(scope="class", autouse=True)
    def my_manifest(self, project, setup):
        yield get_manifest(project.project_root)

    def test_get_one_catalog_by_relations(
        self,
        adapter,
        setup,
        my_information_schema,
        my_seed,
        my_table,
        my_view,
        my_materialized_view,
        my_manifest,
    ):
        my_relations = [my_seed, my_table, my_view, my_materialized_view]
        with get_connection(adapter):
            catalog = adapter._get_one_catalog_by_relations(
                information_schema=my_information_schema,
                relations=my_relations,
                manifest=my_manifest,
            )
        # my_seed, my_table, my_view, my_materialized_view each have 3 cols = 12 cols
        # my_materialized_view creates an underlying table with 2 additional = 5 cols
        # note the underlying table is missing as it's not in `my_relations`
        assert len(catalog) == 12

    def test_get_one_catalog_by_schemas(
        self,
        adapter,
        setup,
        my_information_schema,
        my_schema,
        my_manifest,
    ):
        with get_connection(adapter):
            catalog = adapter._get_one_catalog(
                information_schema=my_information_schema,
                schemas={my_schema.schema},
                manifest=my_manifest,
            )
        # my_seed, my_table, my_view, my_materialized_view each have 3 cols = 12 cols
        # my_materialized_view creates an underlying table with 2 additional = 5 cols
        assert len(catalog) == 17
