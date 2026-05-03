# Sentiment Platform

Real-time sentiment analysis pipeline that ingests social/news content, enriches it with NLP, and processes streams with Kafka + Spark.

## Architecture

- `ingestion/`: producers for Hacker News, NewsAPI, and YouTube
- `nlp_service/`: sentiment + entity enrichment service
- `processing/`: Spark structured streaming job
- `dashboard/`: visualization layer
- `monitoring/`: observability assets
- `storage/`: local output/runtime artifacts (ignored in git)

## Tech Stack

- Python (producers + NLP service)
- Apache Kafka + Zookeeper
- Apache Spark Structured Streaming
- Transformers (`distilbert-base-uncased-finetuned-sst-2-english`)
- spaCy (`en_core_web_sm`)
- Docker Compose

## Quick Start

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r ingestion/requirements.txt`
   - `pip install transformers torch spacy`
   - `python -m spacy download en_core_web_sm`
3. Create environment file:
   - `copy .env.example .env` (Windows)
   - update API keys in `.env`
4. Start infrastructure:
   - `docker compose up -d`
5. Run services from project root:
   - `python ingestion/hacker_news_producer.py`
   - `python ingestion/newsapi_producer.py`
   - `python ingestion/youtube_producer.py`
   - `python nlp_service/enricher.py`
6. Submit Spark job inside container:
   - `docker exec --user root spark /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/spark_streaming.py`

## Security Notes

- Secrets are loaded from `.env` and must not be committed.
- Use `.env.example` as a template for collaborators.
- Rotate API keys immediately if they were ever pushed to a remote.

## Git Hygiene

Before pushing:

- Verify no secrets are staged: `git diff --cached`
- Verify ignored artifacts are not tracked: `git status`
- If files were tracked before ignore rules, untrack them once:
  - `git rm -r --cached venv .env`

## License

Add a LICENSE file before publishing publicly if needed.
