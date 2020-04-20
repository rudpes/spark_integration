from pyspark.sql import SparkSession

warehouseLocation = "C:/Users/Spark/spark-warehouse"


def get_spark_session():
    return SparkSession \
      .builder \
      .appName("app_name") \
      .config("spark.sql.warehouse.dir", warehouseLocation) \
      .config("spark.driver.memory", '2g') \
      .enableHiveSupport() \
      .getOrCreate()