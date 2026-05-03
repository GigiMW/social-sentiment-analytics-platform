# Ingestion

Producers publish raw content into Kafka:

- `hacker_news_producer.py` -> `raw.hackernews`
- `newsapi_producer.py` -> `raw.newsapi`
- `youtube_producer.py` -> `raw.youtube`

Use Docker profile `app` to run all producers:

```bash
docker compose --profile app up -d hackernews-producer newsapi-producer youtube-producer
```
