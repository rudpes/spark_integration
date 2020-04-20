import os
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession

spark_home = os.environ['SPARK_HOME']
logFile = spark_home + "\README.md"

warehouseLocation = "C:/Users/Spark/spark-warehouse"

spark = SparkSession \
  .builder \
  .appName("app_name") \
  .config("spark.sql.warehouse.dir", warehouseLocation) \
  .enableHiveSupport() \
  .getOrCreate()

rootdir = 'E:/RP/csv/target - Copy - Copy - Copy/'

for subdir in os.listdir(rootdir):
  files = rootdir + subdir + '/'
  matchesDF = spark.read \
    .format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load(files + "*.csv")

  matchesDF.write.mode("overwrite").saveAsTable("v2_original_" + subdir)
