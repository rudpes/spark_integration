
# first neural network with keras tutorial
import os

import pandas
from keras.models import Sequential
from keras.layers import Dense
from sklearn.model_selection import train_test_split

from pyspark.sql import SparkSession

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


cont = 80
increment = 10
base_name = "slice_"

start = cont / 100
end = (cont + 10) / 100
#slice_name = base_name + str(cont) + "_" + str(cont + increment)
#slice_df = spark.sql("SELECT * FROM " + slice_name + ' WHERE match_total_duration > 1200')
slice_name = 'union_repository'
slice_name = 'v2_union_repository'

slice_df = spark.sql("SELECT * FROM " + slice_name + ' WHERE match_total_duration > 1200 AND time > 1200 AND duration_percentage > 0.7 AND duration_percentage < 0.9')

removed_cols = ['matchId',
                'player_slot',
                'patch',
#                'gold_earned',
#                'xp_earned',
#                'gold_spent',
#                'match_total_duration',
#                'hero_damage',
#                'hero_healing',
#                'duration_percentage'
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
  .withColumn('result_sentries', slice_df.observer_wards_placed + slice_df.sentry_wards_placed + slice_df.wards_purchased + slice_df.wards_destroyed)
slice_df = slice_df \
  .drop(*['observer_wards_placed', 'sentry_wards_placed', 'wards_purchased', 'wards_destroyed'])

#slice_df = slice_df \
  #.drop(*['total_strength', 'total_agility', 'total_intellect'])

#slice_df = slice_df \
  #.drop(*['kills', 'deaths', 'assists', 'gold_per_min', 'xp_per_min', 'last_hits', 'denies'])

pandas_df = slice_df.toPandas()
print('N of rows: ' + str(len(pandas_df.index)))
categories = ['patch', 'hero_name']
numerical = list(set(slice_df.columns) - set(categories))
pandas_df[numerical] = pandas_df[numerical].apply(lambda x: (x - x.min()) / (x.max() - x.min()))

hero_dummies = pandas.get_dummies(pandas_df.hero_name)
pandas_df = pandas.concat([pandas_df, hero_dummies], axis=1)
pandas_df = pandas_df.drop(['hero_name'], axis=1)


# load the dataset
dataset = pandas_df
for column in dataset.columns:
    print(column)

# split into input (X) and output (y) variables
X = dataset[dataset.columns.difference(['win'])]
y = dataset[['win']]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)

# define the keras model
model = Sequential()
model.add(Dense(100, input_dim=(len(dataset.columns) - 1), activation='relu'))
model.add(Dense(50, activation='relu'))
model.add(Dense(1, activation='sigmoid'))
# compile the keras model
model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
# fit the keras model on the dataset
model.fit(X_train, y_train, epochs=50, batch_size=64)

# evaluate the keras model
_, accuracy = model.evaluate(X_test, y_test)
print('Accuracy: %.2f' % (accuracy*100))
