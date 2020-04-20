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

dfs = []

for df in dfs:
    finalDF = finalDF.union(df)

for const in constants.tables:
    dfs.append(spark.sql("SELECT * FROM v2_original_" + const))

df = reduce(DataFrame.union, dfs)
print(df.count())
df.show()

cont = 0

df.write.mode("overwrite").saveAsTable("v2_union_repository")