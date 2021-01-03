%spark2

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StructField, StructType, StringType}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.hwc.HiveWarehouseSession._

val topics = "twitter"
val brokers = "sandbox-hdp.hortonworks.com:6667"

val spark = SparkSession 
        .builder
        .appName("Twitter Streaming App")
        .master("yarn")
        .enableHiveSupport()
        .config("spark.sql.hive.llap", "true")
        .config("spark.datasource.hive.warehouse.exec.results.max","10000")
        .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default;password=hive;serviceDiscoveryMode=zooKeeper;user=hive;zooKeeperNamespace=hiveserver2")
        .getOrCreate()


val hive = HiveWarehouseSession.session(spark).build()

hive.setDatabase("stockexchange")

hive.createTable("twitter").ifNotExists().column("`date`", "String").column("id", "string").column("source", "string").column("text", "String").column("user_account_created_time", "string").column("user_favourites_count", "int").column("user_followers_count", "int").column("user_friends_count", "int").column("user_id", "string").column("user_name", "string").column("user_statuses_count", "string").create()


val kafkaParams = Map[String,String](
      "metadata.broker.list"->brokers,
      "key.deserializer" -> classOf[StringDeserializer].toString(),
      "value.deserializer"-> classOf[StringDeserializer].toString(),
      "auto.offset.reset" -> "largest"
    )

val batchInterval= Seconds(10)


val ssc = new StreamingContext(spark.sparkContext, batchInterval)
ssc.checkpoint("/warehouse/checkpoint/twitter/")

println("Stream Creating")

val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topics))
      
import spark.implicits._

val lines = messages.map(_._2)

lines.foreachRDD(
      rdd => {

        val rawDF = rdd.toDF("msg")

        if(!rawDF.rdd.isEmpty()) {
          
          val schema = spark.read.json(rawDF.select("msg").as[String]).schema
            
          val df = rawDF.select(from_json($"msg",schema).as("s")).select("s.*")
          
          val df2 = df.withColumn("user_favourites_count",col("user_favourites_count").cast(IntegerType))
                      .withColumn("user_followers_count",col("user_followers_count").cast(IntegerType))
                      .withColumn("user_friends_count",col("user_friends_count").cast(IntegerType))
                      .withColumn("user_statuses_count",col("user_statuses_count").cast(IntegerType))

          
          println("test")
          df2.printSchema()
          df2.show()

          
          println("Saving")
          df2.write.format(HIVE_WAREHOUSE_CONNECTOR).mode("append").option("table", "twitter").save()
          println("Saved")
        }
      }
    )

ssc.start()
ssc.awaitTermination()