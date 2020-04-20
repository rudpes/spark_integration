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
    end = (cont + 10) / 100
    slice_name = "slice_big_" + str(cont) + "_" + str(cont + 20)
    slice_df = spark.sql("SELECT * FROM " + slice_name)

    print("Calculating win percentage of slice: " + slice_name)
    slice_df.groupBy("win") \
        .count() \
        .withColumnRenamed('count', 'cnt_per_group') \
        .withColumn('perc_of_count_total', (F.col('cnt_per_group') / slice_df.count()) * 100) \
        .show(slice_df.count(), truncate=False)

    print("Calculating hero pick percentage of slice: " + slice_name)
    slice_df.groupBy("hero_name") \
        .count() \
        .withColumnRenamed('count', 'cnt_per_group') \
        .withColumn('perc_of_count_total', (F.col('cnt_per_group') / slice_df.count()) * 100) \
        .sort("hero_name") \
        .show(slice_df.count(), truncate=False)

    print("Calculating hero win percentage of slice: " + slice_name)
    slice_df.groupBy("hero_name", 'win') \
        .count() \
        .withColumnRenamed('count', 'cnt_per_group') \
        .withColumn('perc_of_count_total', (F.col('cnt_per_group') / slice_df.count()) * 100) \
        .sort("hero_name") \
        .show(slice_df.count(), truncate=False)

    removed_cols = ['matchId',
                    'player_slot',
                    'patch',
                    'gold_earned',
                    'xp_earned',
                    'gold_spent',
                    #                'match_total_duration',
                    'hero_damage',
                    'hero_healing',
                    'duration_percentage'
                    ]

    slice_df = slice_df.drop(*removed_cols)