import os
import json
import sys

import findspark
from IPython.core.display import display
import pyspark.sql.functions as psf
from pyspark.sql.functions import get_json_object
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DecimalType, DateType, \
    DoubleType, LongType

findspark.init('/Users/pradeepipol/Movies/BigData/spark-3.0.0-bin-hadoop2.7/')
import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext, DStream

#from pyspark.streaming.dstream import d

f_count = 0;


def country_count_update(newValue, existingCount):
    if existingCount is None:
        existingCount = 0

    return sum(newValue) + existingCount
if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0-alpha2 pyspark-shell'


    data_spark_schema =StructType([
    StructField("venue", StructType([
        StructField("venue_name", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("venue_id", DoubleType(), True),

    ]), True),
    StructField("visibility", StringType(), True),
    StructField("response", StringType(), True),
    StructField("guests", LongType(), True),
    StructField("member", StructType([
        StructField("member_id", LongType(), True),
        StructField("photo", StringType(), True),
        StructField("member_name", StringType(), True),
    ]), True),
    StructField("rsvp_id", LongType(), True),
    StructField("mtime", DateType(), True),
    StructField("event", StructType([
        StructField("event_name", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("time", DateType(), True),
        StructField("event_url", StringType(), True),
    ]), True),
    StructField("group", StructType([
        StructField("group_topics", ArrayType(StructType([
            StructField("urlkey", StringType(), True),
            StructField("topic_name", StringType(), True),
        ])), True),
        StructField("group_city", StringType(), True),
        StructField("group_country", StringType(), True),
        StructField("group_id", LongType(), True),
        StructField("group_name", StringType(), True),
        StructField("group_lon", DoubleType(), True),
        StructField("group_lat", DoubleType(), True),
        StructField("group_urlname", StringType(), True),
        StructField("group_state", StringType(), True),
    ]), True),
])



    spark = SparkSession \
       .builder \
       .config("spark.jars","spark-streaming-kafka-0-10_2.12-3.0.0.jar,spark-sql-kafka-0-10_2.12-3.0.0.jar,kafka-clients-2.5.0.jar,commons-pool2-2.8.0.jar,spark-token-provider-kafka-0-10_2.12-3.0.0.jar") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config('spark.cassandra.output.consistency.level', 'ONE') \
        .appName("Q1") \
       .getOrCreate()


    streamingInputDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.1.173:9092") \
        .option("subscribe", "meetup") \
        .load()

    stream_records = streamingInputDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as meetup_data") \
        .select(psf.from_json("meetup_data", data_spark_schema).alias("meetup_data"))
    stream_records.printSchema()
    sites_flat = stream_records.select("meetup_data.*").alias("one").select("member.*","event.*","venue.*","group.*","one.*").drop("group_topics")
    sites_flat1=sites_flat.filter(sites_flat["group_state"]=="CA").select(psf.array_join('group.group_topics.topic_name',delimiter=',').alias("group_topics"),"*").drop("member","event","venue","group")

    sites_flat1.printSchema()


    query =sites_flat1.writeStream\
      .outputMode("append")\
      .format("org.apache.spark.sql.cassandra")\
      .option("checkpointLocation", "Local\Temp")\
      .option("keyspace", "meetup")\
      .option("table", "meet")\
      .start()

    query.awaitTermination()

    #streamingOutPut=streamingInputDF.writeStream.start()


   # print(streamingInputDF.)
   # print(streamingInputDF.status)
    #streamingSelectDF = streamingInputDF.select(get_json_object(($'value').cast("string"),"group_state").alias("state")).groupBy("state")


                                                #(($"value")).cast("string"),"$.group_state")).alias())
    #df.isStreaming()  # Returns True for DataFrames that have streaming sources


  #  print(streamingSelectDF.status)
    #df=pyspark.streaming("kafka").option("kafka.bootstrap.servers", "192.168.56.1:9092").option(
       # "subscribe", "meetup").load()

    #sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount",conf=conf)
    #

   # ssc.checkpoint("checkpoint")


   # kvs = KafkaUtils.createDirectStream(ssc, ["meetup"], {"bootstrap.servers": '192.168.56.1:9092'})

    # , kafkaParams = {"metadata.broker.list":"localhost:9092"}
    #kvs = KafkaUtils.createDirectStream(ssc, topic, kafkaParams)
'''

    kvs = KafkaUtils.createStream(ssc, '192.168.56.1:9092', "1", {'topic': 1})
    rsvps = kvs.map(lambda x: x[1])
    rsvps_json = rsvps.map(lambda x: json.loads(x.encode('ascii', 'ignore')))
    rsvps_us = rsvps_json.filter(lambda x: x['group']['group_country'] == 'us')
    rsvps_city_pair = rsvps_us.map(lambda x: (x['group']['group_city'], 1))
    rsvps_city_statefulCount = rsvps_city_pair.updateStateByKey(country_count_update)

    rsvps_city_statefulCount.pprint()

    ssc.start()
ssc.awaitTermination()
'''
