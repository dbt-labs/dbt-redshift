import pytest

from dbt.tests.util import run_dbt


@pytest.fixture(scope="class", autouse=True)
def run_dbt_results(project):
    yield run_dbt(["run"])


@pytest.fixture
def model_ddl(request) -> str:
    with open(f"target/run/test/models/{request.param}.sql", 'r') as ddl_file:
        ddl_statement = ' '.join(ddl_file.readlines())
        yield ddl_statement.lower()
