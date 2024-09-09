import os

import pytest


@pytest.fixture(autouse=True)
def setup(connection, connection_alt, schema_name) -> str:
    # create the same table in two different databases
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cursor.execute(f"CREATE TABLE {schema_name}.cross_db as select 3.14 as id")
    with connection_alt.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cursor.execute(f"CREATE TABLE {schema_name}.cross_db as select 3.14 as id")

    yield schema_name

    # drop both test schemas
    with connection_alt.cursor() as cursor:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    with connection.cursor() as cursor:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def test_columns_in_relation(connection, schema_name):
    # we're specifically running this query from the default database
    # we're expecting to get both tables, the one in the default database and the one in the alt database
    with connection.cursor() as cursor:
        columns = cursor.get_columns(schema_pattern=schema_name, tablename_pattern="cross_db")

    # we should have the same table in both databases
    assert len(columns) == 2

    databases = set()
    for column in columns:
        (
            database,
            schema,
            table,
            name,
            type_code,
            type_name,
            precision,
            _,
            scale,
            *_,
        ) = column
        databases.add(database)
        assert schema_name == schema_name
        assert table == "cross_db"
        assert name == "id"
        assert type_code == 2
        assert type_name == "numeric"
        assert precision == 3
        assert scale == 2

    # only the databases are different
    assert databases == {
        os.getenv("REDSHIFT_TEST_DBNAME"),
        os.getenv("REDSHIFT_TEST_DBNAME_ALT"),
    }
