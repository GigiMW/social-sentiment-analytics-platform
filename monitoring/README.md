# Monitoring

This folder is reserved for observability assets (Prometheus, Grafana dashboards, alerts).

## Current State

The live dashboard is available via Streamlit at `http://localhost:8501`.
Kafka topic inspection is available through Kafka UI at `http://localhost:8081`.

## Suggested Next Additions

- Prometheus scrape config for service metrics
- Grafana dashboards for throughput and sentiment trend over time
- Alert rules for producer failures and Kafka lag
