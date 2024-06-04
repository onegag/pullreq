from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def spark_session_init(warehouse_path: str) -> SparkSession:
    spark_session = configure_spark_with_delta_pip(
        SparkSession.builder
        .master('local[*]')
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ).getOrCreate()

    return spark_session
