import pytest

from pullreq.sparkSession import spark_session_init


@pytest.fixture(scope="session")
def spark():
    warehouse_path = "./spark_warehouse"
    spark_session = spark_session_init(warehouse_path)
    yield spark_session
    spark_session.stop()
