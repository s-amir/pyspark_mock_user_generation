import json
import logging

import spark as spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("Kafka Event Producer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .master("local[10]") \
    .getOrCreate()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config(filename):
    with open(filename, 'r') as file:
        return json.load(file)


config = load_config('config.json')
kafka_bootstrap_servers = config['kafka']['bootstrap_servers']
user_entrances_topic = config['kafka']['user_entrance_topic']
user_purchases_topic = config['kafka']['user_purchases_topic']

event_schema = StructType([
    StructField("Event_id", StringType(), True),
    StructField("User_id", IntegerType(), True),
    StructField("Timestamp", TimestampType(), True)
])

entrance_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", user_entrances_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10) \
    .option("checkpointLocation",
            "/home/amirhosseindarvishi/PycharmProjects/joinStream/checkpoint1") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), event_schema).alias("data")).select("data.*") \
    .withWatermark("Timestamp", "10 minutes")

purchase_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", user_purchases_topic) \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "/home/amirhosseindarvishi/PycharmProjects/joinStream/checkpoint2") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), event_schema).alias("data")).select("data.*") \
    .withWatermark("Timestamp", "10 minutes")

entrance_df = entrance_df.withColumnRenamed("Event_id", "E_Event_id").withColumnRenamed("User_id",
                                                                                        "E_User_id").withColumnRenamed(
    "Timestamp", "E_Timestamp")

joined_stream = purchase_df.join(entrance_df,
                                 (purchase_df.User_id == entrance_df.E_User_id) &
                                 (purchase_df.Timestamp.between(entrance_df.E_Timestamp - expr("INTERVAL 5 MINUTES"),
                                                                entrance_df.E_Timestamp + expr("INTERVAL 5 MINUTES")))
                                 )

joined_stream.writeStream.format('console') \
    .outputMode("append").option("truncate", "false").start().awaitTermination()
