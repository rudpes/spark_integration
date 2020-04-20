import os
from pyspark import SparkContext
from pyspark import SQLContext
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


for const in constants.tables:
    filtered = spark.sql("SELECT * FROM original_" + const + " WHERE match_total_duration > 1200")

    filtered.write.option("header", "true") \
    .csv("file:///" + output_folder + "reporpoused_" + const + ".csv")
