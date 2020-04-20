import os

import pandas
import xgboost as xgboost
from pyspark.sql import SparkSession, DataFrame
from functools import reduce
import pyspark.sql.functions as F
from sklearn import model_selection
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder

import constants
from dython.nominal import associations

import spark_interface

spark_home = os.environ['SPARK_HOME']
logFile = spark_home + "\README.md"
rootdir = 'E:/RP/csv/target - Copy - Copy/5700/'


warehouseLocation = "C:/Users/Spark/spark-warehouse"
output_folder = 'C:/Users/Spark/outputs/'

spark = spark_interface.get_spark_session()

slice_df = spark_interface

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

#slice_df = slice_df.drop(*['hero_name'])

slice_df = slice_df \
  .withColumn('result_ability_manacost', slice_df.ability_manacost * (1 - (slice_df.manacost_reduction / 100))) \
  .drop(*['ability_manacost', 'manacost_reduction'])

slice_df = slice_df \
  .withColumn('result_cooldown', slice_df.average_cooldown * (1 - (slice_df.cooldown_reduction / 100))) \
  .drop(*['average_cooldown', 'cooldown_reduction'])

slice_df = slice_df \
  .withColumn('result_damage', slice_df.average_damage + slice_df.bonus_damage) \
  .drop(*['average_damage', 'bonus_damage'])

slice_df = slice_df \
  .withColumn('result_ability_offensive', slice_df.ability_damage + slice_df.ability_adds + slice_df.ability_debuff) \
  .drop(*['ability_damage', 'ability_adds', 'ability_debuff'])

slice_df = slice_df \
  .withColumn('result_ability_crowd_control', slice_df.ability_stun + slice_df.ability_silence + slice_df.ability_other_cc + slice_df.ability_slow) \
  .drop(*['ability_stun', 'ability_silence', 'ability_other_cc', 'ability_slow'])

slice_df = slice_df \
  .withColumn('result_ability_support', slice_df.ability_healing + slice_df.ability_buff + slice_df.ability_evasion) \
  .drop(*['ability_healing', 'ability_buff', 'ability_evasion'])

slice_df = slice_df \
  .withColumn('result_sentries', slice_df.observer_wards_placed + slice_df.sentry_wards_placed + slice_df.wards_purchased + slice_df.wards_destroyed) \
  .drop(*['observer_wards_placed', 'sentry_wards_placed', 'wards_purchased', 'wards_destroyed'])

slice_df = slice_df \
  .drop(*['total_strength', 'total_agility', 'total_intellect'])

#df = slice_df \
  #.drop(*['kills', 'deaths', 'assists', 'gold_per_min', 'xp_per_min', 'last_hits', 'denies'])

pandas_df = slice_df.toPandas()

categories = ['patch', 'hero_name']
numerical = list(set(slice_df.columns) - set(categories))
pandas_df[numerical] = pandas_df[numerical].apply(lambda x: (x - x.min()) / (x.max() - x.min()))

hero_dummies = pandas.get_dummies(pandas_df.hero_name)
pandas_df = pandas.concat([pandas_df, hero_dummies], axis=1)
pandas_df = pandas_df.drop(['hero_name'], axis=1)
#associations(pandas_df, theil_u=False, figsize=(len(pandas_df.columns), len(pandas_df.columns)), nominal_columns=categories)

