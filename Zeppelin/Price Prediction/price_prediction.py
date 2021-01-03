%spark2.pyspark

import sys
from pyspark.sql import SparkSession
sc.addPyFile("/usr/hdp/3.0.1.0-187/hive_warehouse_connector/pyspark_hwc-1.0.0.3.0.1.0-187.zip")
from pyspark_llap import HiveWarehouseSession
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from matplotlib import pyplot as plt
from pyspark.sql.types import *
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras import models
from keras import layers
import sklearn.metrics as metrics


spark = SparkSession \
        .builder \
        .appName("Spark Stock Price Prediction") \
        .master("yarn") \
        .enableHiveSupport() \
        .config("spark.sql.hive.llap", "true") \
        .config("spark.yarn.am.cores", "1") \
        .config("spark.datasource.hive.warehouse.exec.results.max","10000") \
        .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default;password=hive;serviceDiscoveryMode=zooKeeper;user=hive;zooKeeperNamespace=hiveserver2") \
        .getOrCreate()

hive = HiveWarehouseSession.session(spark).build()


df = hive.execute("select * from stockexchange.share where ticker='AAPL' AND `date` > '2000-01-01'")


df = df.na.drop()
df = df.withColumn('date', f.to_date('Date'))



date_breakdown = ['year', 'month', 'day']
for i in enumerate(date_breakdown):
    index = i[0]
    name = i[1]
    df = df.withColumn(name, f.split('date', '-')[index])
    
df_plot = df.select('year', 'adjclose').toPandas()

df_plot.set_index('year', inplace=True)
df_plot.plot(figsize=(16, 6), grid=True)
plt.title('Apple stock')
plt.ylabel('Stock Quote ($)')
plt.show()

df.toPandas().shape


trainDF = df[df.year.between(2000, 2018)]
testDF = df[df.year > 2018]

trainDF.toPandas().shape
testDF.toPandas().shape

trainDF_plot = trainDF.select('year', 'adjclose').toPandas()
trainDF_plot.set_index('year', inplace=True)
trainDF_plot.plot(figsize=(16, 6), grid=True)
plt.title('Apple Stock 2000-2018')
plt.ylabel('Stock Quote ($)')
plt.show()

testDF_plot = testDF.select('year', 'adjclose').toPandas()
testDF_plot.set_index('year', inplace=True)
testDF_plot.plot(figsize=(16, 6), grid=True)
plt.title('Apple Stock 2018-2019')
plt.ylabel('Stock Quote ($)')
plt.show()



trainArray = np.array(trainDF.select('Open', 'High', 'Low', 'Close','Volume', 'adjclose' ).collect())

testArray = np.array(testDF.select('Open', 'High', 'Low', 'Close','Volume', 'adjclose' ).collect())


minMaxScale = MinMaxScaler()

minMaxScale.fit(trainArray)
testingArray = minMaxScale.transform(testArray)
trainingArray = minMaxScale.transform(trainArray)


xtrain = trainingArray[:, 0:-1]
xtest = testingArray[:, 0:-1]
ytrain = trainingArray[:, -1:]
ytest = testingArray[:, -1:]

print('xtrain shape = {}'.format(xtrain.shape))
print('xtest shape = {}'.format(xtest.shape))
print('ytrain shape = {}'.format(ytrain.shape))
print('ytest shape = {}'.format(ytest.shape))



plt.figure(figsize=(16,6))
plt.plot(xtrain[:,0],color='red', label='open')
plt.plot(xtrain[:,1],color='blue', label='high')
plt.plot(xtrain[:,2],color='green', label='low')
plt.plot(xtrain[:,3],color='purple', label='close')
plt.legend(loc = 'upper left')
plt.title('Open, High, Low, and Close by Day')
plt.xlabel('Days')
plt.ylabel('Scaled Quotes')
plt.show()


plt.figure(figsize=(16,6))
plt.plot(xtrain[:4],color='black',label='volume')
plt.legend(loc = 'upper right')
plt.title('Volume by Day')
plt.xlabel('Days')
plt.ylabel('Scaled Volume')
plt.show()


model = models.Sequential()
model.add(layers.LSTM(1, input_shape=(1,5)))
model.add(layers.Dense(1))
model.compile(loss='mean_squared_error', optimizer='adam')

xtrain = xtrain.reshape((xtrain.shape[0], 1, xtrain.shape[1]))
xtest = xtest.reshape((xtest.shape[0], 1, xtest.shape[1]))

loss = model.fit(xtrain, ytrain, batch_size=10, epochs=100)

predicted = model.predict(xtest)

combined_array = np.concatenate((ytest, predicted), axis = 1)


plt.figure(figsize=(16,6))
plt.plot(combined_array[:,0],color='red', label='actual')
plt.plot(combined_array[:,1],color='blue', label='predicted')
plt.legend(loc = 'lower right')
plt.title('2018 Actual vs. Predicted APPL Stock')
plt.xlabel('Days')
plt.ylabel('Scaled Quotes')
plt.show()


np.sqrt(metrics.mean_squared_error(ytest,predicted))

df.count()


