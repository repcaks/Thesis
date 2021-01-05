%spark2.ipyspark
import IPython
import sys
from pyspark.sql import SparkSession
sc.addPyFile("/usr/hdp/3.0.1.0-187/hive_warehouse_connector/pyspark_hwc-1.0.0.3.0.1.0-187.zip")
from pyspark_llap import HiveWarehouseSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')
plt.style.use("fivethirtyeight")
from datetime import datetime



spark = SparkSession \
        .builder \
        .appName("Spark Stock Market Analysis") \
        .master("yarn") \
        .enableHiveSupport() \
        .config("spark.sql.hive.llap", "true") \
        .config("spark.yarn.am.cores", "1") \
        .config("spark.datasource.hive.warehouse.exec.results.max","100000") \
        .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default;password=hive;serviceDiscoveryMode=zooKeeper;user=hive;zooKeeperNamespace=hiveserver2") \
        .getOrCreate()

hive = HiveWarehouseSession.session(spark).build()
hive.setDatabase("stockexchange")

df_1 = hive.execute("Select * from share where ticker='CLDR' AND `date` > '2017-01-01'")
df_2 = hive.execute("Select * from cryptocurrency where ticker='BTC-USD' AND `date` > '2017-01-01'")
df_3 = hive.execute("Select * from gold where `date` > '2017-01-01'")
df_4 = hive.execute("Select * from oil where `date` > '2017-01-01'")

names=["Cloudera","Bitcoin","Gold","Oil"]
all_companies_spark= [df_1,df_2,df_3,df_4]

all_companies_pandas = []

for c in all_companies_spark:
    all_companies_pandas.append(c.toPandas())


plt.figure(figsize=(24, 18))
plt.subplots_adjust(top=3.5, bottom=2)
    
for i, company in enumerate(all_companies_pandas, 1):
    plt.subplot(2, 2, i)
    plt.plot(company['date'], company["close"] )
    plt.ylabel('close')
    plt.xlabel('date')
    plt.title(f"{names[i - 1]}")
    

ma_day = [20, 60]

for ma in ma_day:
    for company in all_companies_pandas:
        column_name = f"MA for {ma} days"
        company[column_name] = company['close'].rolling(ma).mean()


fig, axes = plt.subplots(nrows=2, ncols=2)
fig.set_figheight(18)
fig.set_figwidth(24)

all_companies_pandas[0][['close', 'MA for 20 days', 'MA for 60 days']].plot(ax=axes[0,0])
axes[0,0].set_title(names[0])

all_companies_pandas[1][['close', 'MA for 20 days', 'MA for 60 days']].plot(ax=axes[0,1])
axes[0,1].set_title(names[1])

all_companies_pandas[2][['close', 'MA for 20 days', 'MA for 60 days']].plot(ax=axes[1,0])
axes[1,0].set_title(names[2])

all_companies_pandas[3][['close', 'MA for 20 days', 'MA for 60 days']].plot(ax=axes[1,1])
axes[1,1].set_title(names[3])

fig.tight_layout()

for company in all_companies_pandas:
    company['Daily Return'] = company['close'].pct_change()


fig, axes = plt.subplots(nrows=2, ncols=2)
fig.set_figheight(12)
fig.set_figwidth(24)

all_companies_pandas[0]['Daily Return'].plot(ax=axes[0,0], legend=True, linestyle='--', marker='o')
axes[0,0].set_title(names[0])

all_companies_pandas[1]['Daily Return'].plot(ax=axes[0,1], legend=True, linestyle='--', marker='o')
axes[0,1].set_title(names[1])

all_companies_pandas[2]['Daily Return'].plot(ax=axes[1,0], legend=True, linestyle='--', marker='o')
axes[1,0].set_title(names[2])

all_companies_pandas[3]['Daily Return'].plot(ax=axes[1,1], legend=True, linestyle='--', marker='o')
axes[1,1].set_title(names[3])

fig.tight_layout()

plt.figure(figsize=(24, 24))


for i, company in enumerate(all_companies_pandas, 1):
    plt.subplot(2, 2, i)
    sns.distplot(company['Daily Return'].dropna(), bins=100, color='purple')
    plt.ylabel('Daily Return')
    plt.title(f'{names[i - 1]}')
    
    
############################################# Corelation



df_1 = df_1.withColumnRenamed("close","close_cloudera")
df_2 = df_2.withColumnRenamed("close","close_btc")
df_3 = df_3.withColumnRenamed("close","close_gold")
df_4 = df_4.withColumnRenamed("close","close_oil")

df_5 = df_1.alias("c").join(df_2.alias("b"), df_1.date==df_2.date).join(df_3.alias('g'),df_1.date==df_3.date).join(df_4.alias('o'),df_1.date==df_4.date).select('c.date','c.close_cloudera','b.close_btc','g.close_gold','o.close_oil')

z.show(df_5)

pandas_df_correlation=df_5.toPandas()    



sns.jointplot('close_cloudera', 'close_cloudera', pandas_df_correlation, kind='scatter', color='seagreen')
sns.jointplot('close_cloudera', 'close_gold', pandas_df_correlation, kind='scatter', color='orange')
sns.jointplot('close_oil', 'close_gold', pandas_df_correlation, kind='scatter', color='red')


sns.pairplot(pandas_df_correlation, kind='reg')

return_fig = sns.PairGrid(pandas_df_correlation.dropna())

return_fig.map_upper(plt.scatter, color='orange')

return_fig.map_lower(sns.kdeplot, cmap='cool_d')

return_fig.map_diag(plt.hist, bins=30)
