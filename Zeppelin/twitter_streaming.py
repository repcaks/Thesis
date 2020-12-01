%spark2.pyspark

from pyspark.sql import SparkSession
sc.addPyFile("/usr/hdp/3.0.1.0-187/hive_warehouse_connector/pyspark_hwc-1.0.0.3.0.1.0-187.zip")
from pyspark_llap import HiveWarehouseSession
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *


KAFKA_TOPIC_NAME_CONS = "twitter"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'sandbox-hdp.hortonworks.com:6667'


spark = SparkSession \
        .builder \
        .appName("Spark Stock Price Prediction") \
        .master("yarn") \
        .enableHiveSupport() \
        .config("spark.sql.hive.llap", "true") \
        .config("spark.datasource.hive.warehouse.exec.results.max","10000") \
        .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default;password=hive;serviceDiscoveryMode=zooKeeper;user=hive;zooKeeperNamespace=hiveserver2") \
                .config("spark.jars",
                "/jars/kafka-clients-1.1.0.jar/spark-sql-kafka-0-10_2.11-2.3.2.3.1.0.6-1.jar,/jars/kafka-clients-1.1.0.jar/SparkWarsawRealTimePipeline//kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraClassPath",
                "/jars/kafka-clients-1.1.0.jar/spark-sql-kafka-0-10_2.11-2.3.2.3.1.0.6-1.jar:/jars/kafka-clients-1.1.0.jar/SparkWarsawRealTimePipeline//kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraLibrary",
                "/jars/kafka-clients-1.1.0.jar/spark-sql-kafka-0-10_2.11-2.3.2.3.1.0.6-1.jar:/jars/kafka-clients-1.1.0.jar/SparkWarsawRealTimePipeline//kafka-clients-1.1.0.jar") \
        .config("spark.driver.extraClassPath",
               "/jars/kafka-clients-1.1.0.jar/spark-sql-kafka-0-10_2.11-2.3.2.3.1.0.6-1.jar:/jars/kafka-clients-1.1.0.jar/SparkWarsawRealTimePipeline//kafka-clients-1.1.0.jar") \
        .getOrCreate()

hive = HiveWarehouseSession.session(spark).build()

spark.sparkContext.setLogLevel("ERROR")

transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()
        
print("Printing Schema of transaction_detail_df: ")
transaction_detail_df.printSchema()

transaction_detail_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

kafkaStream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
    .load()
kafkaStream.printSchema()
        

