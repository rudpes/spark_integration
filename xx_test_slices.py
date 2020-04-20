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

for tree_count in range(64, 73):
    for depth_cont in range(20, 31):
        for cont in range(70, 110, 10):
            start = cont / 100
            end = (cont + 10) / 100
            slice_name = "slice_" + str(cont) + "_" + str(cont + 10)
            n_trees = 64
            depth = depth_cont
            model_random_forest.random_forest_implementation(slice_name, n_trees, depth, spark)