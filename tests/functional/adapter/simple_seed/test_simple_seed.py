import pytest

from dbt.tests.adapter.seed.test_seed import (
    BasicSeedTests,
    SeedConfigFullRefreshOn,
    SeedConfigFullRefreshOff,
    SeedCustomSchema,
    SimpleSeedEnabledViaConfig,
    SeedParsing,
    SimpleSeedWithBOM,
    SeedSpecificFormats
)
from dbt.tests.adapter.seed.test_seed_type_override import SimpleSeedColumnOverride
from fixtures_local import properties__schema_yml


class TestBasicSeedTests(BasicSeedTests):
    pass


class TestSeedConfigFullRefreshOn(SeedConfigFullRefreshOn):
    pass


class TestSeedConfigFullRefreshOff(SeedConfigFullRefreshOff):
    pass


class TestSeedCustomSchema(SeedCustomSchema):
    pass


class TestSimpleSeedEnabledViaConfig(SimpleSeedEnabledViaConfig):
    pass


class TestSeedParsing(SeedParsing):
    pass


class TestSimpleSeedWithBOM(SimpleSeedWithBOM):
    pass


class TestSeedSpecificFormats(SeedSpecificFormats):
    pass


class TestSimpleSeedColumnOverride(SimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": properties__schema_yml,
        }
