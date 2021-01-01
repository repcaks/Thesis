# zeppelin interpreter: %spark2.pyspark
# csv files are in file:///Data/ 
# orc files are created in HDFS /user/hive/tmp/data/
# command to list files in hdfs :  hdfs dfs -ls /user/hive/tmp/data/   
#df.printSchema() --> to check if generated orc file has correct schema


df1 = sqlContext.read.load('file:///Data/shares.csv', 
                      format='csv', 
                      header='true', 
                      inferSchema='true')

df1.printSchema()

df1.write.save("/user/hive/tmp/data/shares.orc", format="orc")

# News data has ";" as delimiter

from pyspark.sql.functions import *

df2 = spark.read.option("delimiter", ";").csv('file:///Data/news.csv')

df2.printSchema()
df2.write.save("/user/hive/tmp/data/news.orc", format="orc")


df3 = sqlContext.read.load('file:///Data/Oil.csv', 
                      format='csv', 
                      header='true', 
                      inferSchema='true')

df3.printSchema()

df3.write.save("/user/hive/tmp/data/oil.orc", format="orc")

df4 = sqlContext.read.load('file:///Data/companies.csv', 
                      format='csv', 
                      header='false', 
                      inferSchema='true')

df4.printSchema()

df4.write.save("/user/hive/tmp/data/companies.orc", format="orc")


df5 = sqlContext.read.load('file:///Data/Gold.csv', 
                      format='csv', 
                      header='true', 
                      inferSchema='true')

df5.printSchema()

df5.write.save("/user/hive/tmp/data/gold.orc", format="orc")


df6 = sqlContext.read.load('file:///Data/Dates.csv', 
                      format='csv', 
                      header='true', 
                      inferSchema='true')

df6.printSchema()

df6.write.save("/user/hive/tmp/data/dates.orc", format="orc")


df7 = sqlContext.read.load('file:///Data/Covid.csv', 
                      format='csv', 
                      header='true', 
                      inferSchema='true')

df7.printSchema()

df7.write.save("/user/hive/tmp/data/covid.orc", format="orc")



df8 = sqlContext.read.load('file:///Data/Crypto.csv', 
                      format='csv', 
                      header='true', 
                      inferSchema='true')

df8.printSchema()

df8.write.save("/user/hive/tmp/data/crypto.orc", format="orc")

df9 = sqlContext.read.load('file:///Data/incomeStatements.csv', 
                      format='csv', 
                      header='false', 
                      inferSchema='true')

df9.printSchema()

df9.write.save("/user/hive/tmp/data/incomeStatements.orc", format="orc")
