from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, to_json, struct, window
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
INPUT_TOPIC = "enriched.nlp"
OUTPUT_TOPIC = "analytics.sentiment"
CHECKPOINT_BASE = "/opt/spark-apps/checkpoints"

spark = (
    SparkSession.builder.appName("SentimentPlatformStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("text", StringType(), True),
        StructField("author", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("url", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("sentiment", StringType(), True),
        StructField("confidence", DoubleType(), True),
        StructField(
            "entities",
            ArrayType(
                StructType(
                    [
                        StructField("text", StringType(), True),
                        StructField("label", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("enriched", BooleanType(), True),
    ]
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp"),
).select("data.*", "kafka_timestamp")

valid_df = parsed_df.filter(col("id").isNotNull() & col("source").isNotNull())

aggregated_df = (
    valid_df.withWatermark("kafka_timestamp", "2 minutes")
    .groupBy(window(col("kafka_timestamp"), "5 minutes"), col("source"), col("sentiment"))
    .agg(
        count("*").alias("post_count"),
        avg(col("confidence")).alias("avg_confidence"),
    )
    .select(
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("source"),
        col("sentiment"),
        col("post_count"),
        col("avg_confidence"),
    )
)

analytics_kafka_df = aggregated_df.select(
    to_json(
        struct(
            col("window_start"),
            col("window_end"),
            col("source"),
            col("sentiment"),
            col("post_count"),
            col("avg_confidence"),
        )
    ).alias("value")
)

console_events_query = (
    valid_df.select("id", "source", "topic", "sentiment", "confidence", "text")
    .writeStream.outputMode("append")
    .format("console")
    .option("truncate", True)
    .option("numRows", 20)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/events_console")
    .start()
)

console_agg_query = (
    aggregated_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/agg_console")
    .start()
)

kafka_out_query = (
    analytics_kafka_df.writeStream.outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/analytics_kafka")
    .start()
)

spark.streams.awaitAnyTermination()
