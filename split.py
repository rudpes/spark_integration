import os
from pyspark.sql import SparkSession

import constants

spark_home = os.environ['SPARK_HOME']
logFile = spark_home + "\README.md"
rootdir = 'E:/RP/csv/target - Copy - Copy/5700/'


warehouseLocation = "C:/Users/Spark/spark-warehouse"
output_folder = 'C:/Users/Spark/outputs/'

spark = SparkSession \
  .builder \
  .appName("app_name") \
  .config("spark.sql.warehouse.dir", warehouseLocation) \
  .enableHiveSupport() \
  .getOrCreate()

minimal_cols = constants.unique_columns + constants.main_columns
abilities_cols = constants.unique_columns + constants.abilities_columns
items_cols = constants.unique_columns + constants.items_columns

for const in constants.tables:
    filtered = spark.sql("SELECT * FROM filtered_" + const)
    filtered.select(minimal_cols).write.mode("overwrite").saveAsTable("minimal_" + const)
    filtered.select(abilities_cols).write.mode("overwrite").saveAsTable("abilities_" + const)
    filtered.select(items_cols).write.mode("overwrite").saveAsTable("items_" + const)