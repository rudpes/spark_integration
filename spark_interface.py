import os

from pyspark.sql import SparkSession

warehouseLocation = "C:/Users/Spark/spark-warehouse"
spark_home = os.environ['SPARK_HOME']
logFile = spark_home + "\README.md"


def get_spark_session():
    return SparkSession \
      .builder \
      .appName("app_name") \
      .config("spark.sql.warehouse.dir", warehouseLocation) \
      .config("spark.driver.memory", '2g') \
      .enableHiveSupport() \
      .getOrCreate()


def get_union_repo(spark):
    return spark.sql('SELECT * FROM union_repository WHERE match_total_duration > 1200')


def get_union_repo_v2(spark):
    return spark.sql('SELECT * FROM v2_union_repository WHERE match_total_duration > 1200')


def get_slice_v2(spark):
    return spark.sql('SELECT * FROM v2_union_repository WHERE match_total_duration > 1200')


def get_slice_big_v2(spark):
    return spark.sql('SELECT * FROM v2_union_repository WHERE match_total_duration > 1200')


def get_slice(spark, start_pct):
    return resolve_slice(spark, 'slices_', start_pct)


def get_slice_big(spark, start_pct):
    return resolve_slice(spark, 'slices_big_', start_pct)


def resolve_slice(spark, base_name, start_pct):
    increment = 10
    slice_name = base_name + str(start_pct) + "_" + str(start_pct + increment)
    return spark.sql("SELECT * FROM " + slice_name + ' WHERE match_total_duration > 1200')


def resolve_slice_big(spark, base_name, start_pct):
    increment = 20
    slice_name = base_name + str(start_pct) + "_" + str(start_pct + increment)
    return spark.sql("SELECT * FROM " + slice_name + ' WHERE match_total_duration > 1200')


def resolve_custom_slice(spark, table, start, increment):
    start = start / 100
    end = (start + increment) / 100
    where_clause = " WHERE duration_percentage > " + str(start) + " AND duration_percentage < " + str(end)
    return spark.sql("SELECT * FROM " + table + where_clause)


def get_table(spark, table):
    return spark.sql("SELECT * FROM " + table + ' WHERE match_total_duration > 1200')
