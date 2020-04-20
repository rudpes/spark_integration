import inline as inline
import matplotlib
import numpy
import pandas
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

import constants
import spark_interface

import seaborn as sns
import matplotlib.pyplot as plt

sns.set_style("whitegrid")
sns.set(rc = {'figure.figsize':(15, 10)})

# udfs ----

# function for creating a feature importance dataframe
def imp_df(column_names, importances):
    df = pandas.DataFrame({'Feature': column_names,
                       'Information_Gain': importances}) \
           .sort_values('Information_Gain', ascending = False) \
           .reset_index(drop = True)
    return df

# plotting a feature importance dataframe (horizontal barchart)
def var_imp_plot(imp_df, title):
    imp_df.columns = ['Feature', 'Information_Gain']
    sns.barplot(x = 'Information_Gain', y = 'feature', data = imp_df, orient = 'h', color = 'royalblue') \
       .set_title(title, fontsize = 20)

spark = spark_interface.get_spark_session()

end_table = spark_interface.get_table(spark, 'v2_union_repository')
cols = constants.items_columns + constants.abilities_columns
cols = cols + ['player_slot', 'matchId']
end_table = end_table.drop(*cols)

end_table.show()

pandas_df = end_table.toPandas()
hero_dummies = pandas.get_dummies(pandas_df.hero_name)
pandas_df = pandas.concat([pandas_df, hero_dummies], axis=1)
pandas_df = pandas_df.drop(['hero_name'], axis=1)

X = pandas_df[pandas_df.columns.difference(['win'])]
y = pandas_df[['win']]

X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size = 0.8, random_state = 42)

rf = RandomForestRegressor(n_estimators = 100,
                           n_jobs = -1,
                           oob_score = True,
                           bootstrap = True,
                           random_state = 42)
rf.fit(X_train, y_train)

base_imp = imp_df(X_train.columns, rf.Information_Gain)
print(base_imp)
var_imp_plot(base_imp, 'Default feature importance (scikit-learn)')
plt.rcParams["figure.figsize"] = (20,20)
plt.show()