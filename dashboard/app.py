import json
from collections import deque

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPICS = ["enriched.nlp", "analytics.sentiment"]
MAX_EVENTS = 200

st.set_page_config(page_title="Sentiment Platform Dashboard", layout="wide")
st.title("Real-Time Sentiment Dashboard")
st.caption("Live view of enriched and aggregated sentiment events from Kafka")


@st.cache_resource
def get_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer",
    )


consumer = get_consumer()
items = deque(maxlen=MAX_EVENTS)
for _ in range(50):
    msg_pack = consumer.poll(timeout_ms=20)
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
