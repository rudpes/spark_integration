import os
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession

import constants

spark_home = os.environ['SPARK_HOME']
logFile = spark_home + "\README.md"
rootdir = 'E:/RP/csv/target - Copy - Copy/5700/'


warehouseLocation = "C:/Users/Spark/spark-warehouse"
spark = SparkSession \
  .builder \
  .appName("app_name") \
  .config("spark.sql.warehouse.dir", warehouseLocation) \
  .enableHiveSupport() \
  .getOrCreate()

for const in constants.tables:

    filtered = spark.sql("SELECT * FROM original_" + const + " "
                         "WHERE human_players = 10 "
                         "AND lobby_type IN (0, 2, 5, 6, 7) "
                         "AND game_mode IN (1, 2, 4, 12, 13, 14, 16, 22) "
                         "AND leaver_status = 0")
    removalList = ['human_players', 'lobby_type', 'game_mode', 'leaver_status']

    filtered = filtered.drop(*removalList)
    filtered.write.mode("overwrite").saveAsTable("filtered_" + const)
