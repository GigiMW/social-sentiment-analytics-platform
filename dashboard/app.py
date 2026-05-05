import json
import os
from collections import deque

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPICS = ["enriched.nlp", "analytics.sentiment"]
MAX_EVENTS = 200

st.set_page_config(page_title="Sentiment Platform Dashboard", layout="wide")
st.title("Real-Time Sentiment Dashboard")
st.caption("Live view of enriched and aggregated sentiment events from Kafka")


@st.cache_resource
def get_consumer() -> KafkaConsumer:
    consumer_kwargs = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
        "auto_offset_reset": "earliest",
        "group_id": "dashboard-consumer",
    }
    
    # Add SASL auth if credentials provided (for managed Kafka like Confluent Cloud)
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        consumer_kwargs.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })
    
    return KafkaConsumer(*TOPICS, **consumer_kwargs)


consumer = get_consumer()
# Seek to beginning to read all available messages
consumer.poll(timeout_ms=1)  # trigger partition assignment
for topic_partition in consumer.assignment():
    consumer.seek_to_beginning(topic_partition)

items = deque(maxlen=MAX_EVENTS)
# Poll for up to 10 seconds to give Kafka time to deliver messages
for _ in range(100):
    msg_pack = consumer.poll(timeout_ms=100)
    for messages in msg_pack.values():
        for msg in messages:
            payload = msg.value
            payload["topic_name"] = msg.topic
            items.append(payload)

if not items:
    st.info("No events yet. Start producers + nlp_service + spark job and refresh.")
    st.stop()

df = pd.DataFrame(list(items))

col1, col2, col3 = st.columns(3)
col1.metric("Events buffered", len(df))
col2.metric("Unique sources", df.get("source", pd.Series(dtype=str)).nunique())
col3.metric(
    "Sentiment labels",
    df[df.get("sentiment").notna()]["sentiment"].nunique() if "sentiment" in df else 0,
)

if "sentiment" in df.columns:
    sentiment_counts = df["sentiment"].fillna("unknown").value_counts().reset_index()
    sentiment_counts.columns = ["sentiment", "count"]
    fig = px.bar(sentiment_counts, x="sentiment", y="count", title="Sentiment Distribution")
    st.plotly_chart(fig, use_container_width=True)

if {"source", "sentiment"}.issubset(df.columns):
    source_sentiment = (
        df[["source", "sentiment"]]
        .fillna("unknown")
        .groupby(["source", "sentiment"])
        .size()
        .reset_index(name="count")
    )
    fig2 = px.bar(
        source_sentiment,
        x="source",
        y="count",
        color="sentiment",
        title="Sentiment by Source",
    )
    st.plotly_chart(fig2, use_container_width=True)

st.subheader("Latest Events")
st.dataframe(df.tail(20), use_container_width=True)
