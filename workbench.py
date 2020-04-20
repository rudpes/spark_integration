import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from functools import reduce

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
    filtered = spark.sql("SELECT * FROM filtered_" + const)

    print("Calculating win percentage of slice: " + str(const))
    filtered.groupBy("win") \
        .count() \
        .withColumnRenamed('count', 'cnt_per_group') \
        .withColumn('perc_of_count_total', (F.col('cnt_per_group') / filtered.count()) * 100) \
        .show(filtered.count(), truncate=False)

    print("Calculating hero pick percentage of slice: " + str(const))
    filtered.groupBy("hero_name") \
        .count() \
        .withColumnRenamed('count', 'cnt_per_group') \
        .withColumn('perc_of_count_total', (F.col('cnt_per_group') / filtered.count()) * 100) \
        .sort("hero_name") \
        .show(filtered.count(), truncate=False)

    print("Calculating hero win percentage of slice: " + str(const))
    filtered.groupBy("hero_name", 'win') \
        .count() \
        .withColumnRenamed('count', 'cnt_per_group') \
        .withColumn('perc_of_count_total', (F.col('cnt_per_group') / filtered.count()) * 100) \
        .sort("hero_name") \
        .show(filtered.count(), truncate=False)