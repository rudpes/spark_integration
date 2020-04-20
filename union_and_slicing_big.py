import os
from pyspark.sql import SparkSession, DataFrame
from functools import reduce
import pyspark.sql.functions as F

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


for cont in range(0, 110, 20):
    start = cont / 100
    end = (cont + 20) / 100
    where_clause = " WHERE duration_percentage > " + str(start) + " AND duration_percentage < " + str(end)
    slice_df = spark.sql("SELECT * FROM union_repository" + where_clause)
    slice_name = "slice_big_" + str(cont) + "_" + str(cont + 20)
    slice_df.write.mode("overwrite").saveAsTable(slice_name)
