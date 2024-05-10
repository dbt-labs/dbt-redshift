from dbt.adapters.capability import Capability, CapabilitySupport, Support
from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.catalog_tests import files


class BaseGetCatalog:
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


class TestGetCatalogByRelations(BaseGetCatalog):
    @pytest.mark.flaky
    def test_get_one_catalog_by_relations(self, project, adapter):
        project.adapter._capabilities[Capability.SchemaMetadataByRelations] = CapabilitySupport(
            support=Support.Full
        )
        results = run_dbt(["docs", "generate"])
        assert len(results.nodes) == 4
        assert project.adapter._capabilities[
            Capability.SchemaMetadataByRelations
        ] == CapabilitySupport(support=Support.Full)


class TestGetCatalogBySchemas(BaseGetCatalog):
    @pytest.mark.flaky
    def test_get_one_catalog_by_schemas(self, project, adapter):
        project.adapter._capabilities[Capability.SchemaMetadataByRelations] = CapabilitySupport(
            support=Support.NotImplemented
        )
        results = run_dbt(["docs", "generate"])
        assert len(results.nodes) == 4
        assert project.adapter._capabilities[
            Capability.SchemaMetadataByRelations
        ] == CapabilitySupport(support=Support.NotImplemented)
