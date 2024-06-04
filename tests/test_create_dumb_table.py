import pytest

from create_dumb_table import Creator
from spark_session import spark_session_init


@pytest.fixture(scope="session")
def spark():
    warehouse_path = "./spark_warehouse"
    spark_session = spark_session_init(warehouse_path)
    yield spark_session
    spark_session.stop()

def test_create_table(spark):
    creator = Creator()
    table_name = "test_table"
    creator.create_table(table_name)

    # Verify the table was created
    tables = spark.catalog.listTables()
    table_names = [table.name for table in tables]
    assert table_name in table_names, "Table was not created."

    # Verify the data was inserted
    result = spark.sql(f"SELECT * FROM {table_name}").collect()
    assert len(result) == 1, "Data was not inserted."
    assert result[0]['id'] == 1, "Inserted data id does not match."
    assert result[0]['value'] == 'data', "Inserted data value does not match."