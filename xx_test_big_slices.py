from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoderEstimator

import constants
from pyspark.sql import SparkSession

import model_random_forest

warehouseLocation = "C:/Users/Spark/spark-warehouse"
spark = SparkSession \
  .builder \
  .appName("app_name") \
  .config("spark.sql.warehouse.dir", warehouseLocation) \
  .config("spark.driver.memory", '2g') \
  .enableHiveSupport() \
  .getOrCreate()

for cont in range(0, 110, 20):
    start = cont / 100
    end = (cont + 20) / 100
    slice_name = "slice_big_" + str(cont) + "_" + str(cont + 20)
    n_trees = 20
    depth = 35
    model_random_forest.random_forest_implementation(slice_name, n_trees, depth, spark)