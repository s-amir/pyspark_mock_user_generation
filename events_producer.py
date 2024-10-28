# import json
# import logging
# import threading
# from datetime import datetime
# import random
# from pyspark.sql import SparkSession
#
# # Initialize Spark Session
# from pyspark.sql.types import StructType, StringType, StructField
#
# spark = SparkSession.builder \
#     .appName("Kafka Event Producer") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
#     .getOrCreate()
#
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#
# def load_config(filename):
#     with open(filename, 'r') as file:
#         config = json.load(file)
#     return config
#
#
# # Load configuration
# config = load_config('config.json')
#
# # Accessing configuration values
# kafka_bootstrap_servers = config['kafka']['bootstrap_servers']
# user_clicks_topic = config['kafka']['user_entrance_topic']
# user_purchases_topic = config['kafka']['user_purchases_topic']
#
#
# def generate_entrance_event(user_id, ):
#     return {"Event_id": "entrance",
#             "User_id": user_id,
#             "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
#
#
# def generate_purchase_event(user_id):
#     return {"Event_id": "purchase",
#             "User_id": user_id,
#             "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M%S")
#
#             }
#
#
# def generate_user_id(user_id_range_degree=5, initial_number=10):
#     return random.randint(0, initial_number * user_id_range_degree)
#
#
# def purchase_or_not(purchase_ratio):
#     if purchase_ratio > random.random():
#         return True
#     else:
#         return False
#
#
# def produce_event(purchase_ratio=0.3):
#     event_schema = StructType([
#         StructField("key", StringType(), True),
#         StructField("value", StringType(), True)
#     ])
#     while True:
#         user_id = generate_user_id()
#         entrance_event = spark.createDataFrame([(None, json.dumps(generate_entrance_event(user_id)))], event_schema)
#         purchase = purchase_or_not(purchase_ratio)
#
#         entrance_event.write \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#             .option("topic", user_clicks_topic) \
#             .save()
#
#         if purchase:
#             purchase_event = spark.createDataFrame([(None, generate_purchase_event(user_id))], event_schema)
#             purchase_event.write \
#                 .format("kafka") \
#                 .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#                 .option("topic", user_purchases_topic) \
#                 .save()
#
#         logging.log(level=logging.INFO,msg=f"purchase is {purchase}")
#
#
# def start_producing():
#     thread = threading.Thread(target=produce_event())
#     thread.start()
#     return thread
#
#
# threads = []
# for _ in range(0, 10):
#     thread = start_producing()
#     threads.append(thread)
#
# for thread in threads:
#     print(str(thread) + " Joined")
#     thread.join()


# ____________________________________________________________________________________
import json
import logging
import random
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, Row
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka Event Producer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .master("local[2]")\
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
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })


def generate_purchase_event(user_id):
    return json.dumps({
        "Event_id": "purchase",
        "User_id": user_id,
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    time.sleep(1000)


# Start streaming to Kafka
start_kafka_stream(num_users=1000000, purchase_ratio=0.3)

