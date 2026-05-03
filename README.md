# Real-Time Social Sentiment and Market Intelligence Platform

An end-to-end streaming platform that ingests social/news data, enriches it with NLP, and produces real-time sentiment analytics.

## What This Project Demonstrates

- Multi-source data ingestion with Kafka producers
- NLP enrichment using Transformers + spaCy
- Stream processing with Spark Structured Streaming
- Real-time analytics dashboard with Streamlit
- Containerized local environment with Docker Compose

## Architecture

- `ingestion/`: Hacker News, NewsAPI, and YouTube Kafka producers
- `nlp_service/`: sentiment and entity enrichment service
- `processing/`: Spark streaming aggregations (`enriched.nlp` -> `analytics.sentiment`)
- `dashboard/`: Streamlit dashboard for live sentiment tracking
- `monitoring/`: observability notes/placeholders

## Quick Start

1. Create env file:
   - `copy .env.example .env` (Windows)
2. Start infra + app services:
   - `docker compose --profile app up -d --build`
3. Submit Spark job:
   - `docker exec --user root spark /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/spark_streaming.py`
4. Open UIs:
   - Kafka UI: `http://localhost:8081`
   - Streamlit Dashboard: `http://localhost:8501`
   - Spark Master UI: `http://localhost:8090`

## Smoke Test

Run an automated health check for containers, endpoints, and Kafka data flow:

- `powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1`

Useful flags:

- `-SkipBuild`: skip image rebuild when containers already exist
- `-RunSparkSubmit`: start Spark streaming job in background as part of the smoke test
- `-RequireAnalytics`: fail if `analytics.sentiment` has no messages

## Core Kafka Topics

- Raw topics:
  - `raw.hackernews`
  - `raw.newsapi`
  - `raw.youtube`
- Enriched topic:
  - `enriched.nlp`
- Aggregated analytics topic:
  - `analytics.sentiment`

## Environment Variables

Required in `.env`:

- `KAFKA_BOOTSTRAP_SERVERS`
- `NEWS_API_KEY`
- `YOUTUBE_API_KEY`

Use `.env.example` as the template for local setup.

## Project Status

This repository is runnable locally and structured for portfolio presentation. Future upgrades can include persistent storage sinks, Prometheus/Grafana dashboards, and automated tests in CI.
