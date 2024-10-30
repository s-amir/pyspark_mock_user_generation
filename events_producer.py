
import json
import logging
import random
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, Row

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka Event Producer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .master("local[10]") \
    .getOrCreate()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Load configuration from JSON file
def load_config(filename):
    with open(filename, 'r') as file:
        return json.load(file)


config = load_config('config.json')
kafka_bootstrap_servers = config['kafka']['bootstrap_servers']
user_clicks_topic = config['kafka']['user_entrance_topic']
user_purchases_topic = config['kafka']['user_purchases_topic']


# Helper functions for event generation
def generate_entrance_event(user_id):
    return json.dumps({
        "Event_id": "entrance",
        "User_id": user_id,
        "Timestamp": (datetime.now() - timedelta(seconds=random.randint(-10, 10))).strftime("%Y-%m-%d %H:%M:%S")
    })


def generate_purchase_event(user_id):
    return json.dumps({
        "Event_id": "purchase",
        "User_id": user_id,
        "Timestamp": (datetime.now() - timedelta(seconds=random.randint(-10, 10))).strftime("%Y-%m-%d %H:%M:%S")
    })


event_schema = StructType([
    StructField("entrance_event", StringType(), True),
    StructField("purchase_event", StringType(), True)
])


# Define the UDF with the StructType return type
@F.udf(returnType=event_schema)
def generate_events(user_id, is_purchase):
    entrance_event = generate_entrance_event(user_id)  # Assuming this returns a JSON string
    purchase_event = generate_purchase_event(user_id) if is_purchase else None

    # Create a Row matching the StructType
    return Row(entrance_event=entrance_event, purchase_event=purchase_event)


# Generate initial DataFrame with user IDs and event types
def generate_events_dataframe(num_users, purchase_ratio):
    user_ids = [random.randint(1, 1000) for _ in range(num_users)]
    events_df = spark.createDataFrame([(user_id, random.random() <= purchase_ratio) for user_id in user_ids],
                                      ["user_id", "purchase"])
    events_df = events_df.withColumn("event", generate_events(col("user_id"), col("purchase")))
    return events_df


# Structured Streaming to Kafka
def start_kafka_stream(num_users, purchase_ratio):
    # Create DataFrame of events
    events_df = generate_events_dataframe(num_users, purchase_ratio)
    events_df.persist()
    events_entrance_df = events_df.select(
        F.col("event.entrance_event").alias("entrance_event")
    ).filter(
        F.col("entrance_event").isNotNull()
    )
    events_purchase_df = events_df.select(
        F.col("event.purchase_event").alias("purchase_event")
    ).filter(
        F.col("purchase_event").isNotNull()
    )
    # Write entrance events to Kafka

    events_entrance_df.selectExpr("CAST(null AS STRING) AS key", "entrance_event AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", user_clicks_topic) \
        .save()

    # Write purchase events to Kafka
    events_purchase_df.selectExpr("CAST(null AS STRING) AS key", "purchase_event AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", user_purchases_topic) \
        .save()


# Start streaming to Kafka
while True:
    purchase_ratio = random.random()
    print("purchase_ratio="+str(purchase_ratio))
    start_kafka_stream(num_users=3, purchase_ratio=purchase_ratio)
    time.sleep(2)
