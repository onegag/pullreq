from pyspark.sql import SparkSession


class Creator:

    def __init__(self):
        spark_session = SparkSession.getActiveSession()
        assert spark_session is not None
        self.spark = spark_session

    def create_table(self, table: str):
        # SQL statement to create the table
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT,
                value STRING
            ) USING DELTA
            """
        self.spark.sql(create_table_sql)

        insert_data_sql = f"""
            INSERT INTO {table} VALUES
            (1, 'data')
            """
        self.spark.sql(insert_data_sql)
