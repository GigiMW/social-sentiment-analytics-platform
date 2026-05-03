import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

spark = SparkSession.builder \
    .appName("SentimentPlatformStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id",         StringType(),  True),
    StructField("source",     StringType(),  True),
    StructField("text",       StringType(),  True),
    StructField("author",     StringType(),  True),
    StructField("topic",      StringType(),  True),
    StructField("url",        StringType(),  True),
    StructField("created_at", StringType(),  True),
    StructField("score",      IntegerType(), True),
    StructField("like_count", IntegerType(), True),
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "raw.hackernews,raw.newsapi,raw.youtube") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

deduped_df = parsed_df.dropDuplicates(["id"])

windowed_df = deduped_df \
    .withWatermark("kafka_timestamp", "2 minutes") \
    .groupBy(
        window("kafka_timestamp", "5 minutes"),
        col("source"),
        col("topic")
    ).agg(count("*").alias("post_count"))

console_query = deduped_df \
    .select("id", "source", "topic", "text", "author")