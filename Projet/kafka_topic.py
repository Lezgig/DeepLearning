from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

def main():
  # Connect to Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            api_version=(0,11,5),
                            value_serializer=lambda x: dumps(x).encode('utf-8'))

  # Connect to local Spark instance
    spark = SparkSession.builder.appName("CapteursDataStorage").getOrCreate()

  # Define schema for Capteurs data
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    # Define Kafka topic to read from
    topic = "donnees_capteurs"

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    query = df \
        .writeStream \
        .format("parquet") \
        .option("path", "capteurs_data_storage") \
        .option("checkpointLocation", "capteurs_data_storage_checkpoint") \
        .start()
    
if __name__ == "__main__":
    main()