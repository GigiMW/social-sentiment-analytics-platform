# Processing

Run Spark aggregation inside the Spark container:

```bash
docker exec --user root spark /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/spark_streaming.py
```

The job reads `enriched.nlp` and publishes aggregated metrics to `analytics.sentiment`.
