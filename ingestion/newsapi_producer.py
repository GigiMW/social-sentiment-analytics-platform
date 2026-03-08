import json
import time
import hashlib
from newsapi import NewsApiClient
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
TOPIC = "raw.newsapi"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

newsapi = NewsApiClient(api_key=NEWS_API_KEY)

TOPICS = ["technology", "business", "science", "health", "general"]
def run():
    seen = set()
    print("Starting NewsAPI producer...")
    while True:
        for topic in TOPICS:
            try:
                articles = newsapi.get_top_headlines(
                    category=topic,
                    language="en",
                    page_size=10
                )
                for article in articles.get("articles", []):
                    # create unique id from url
                    article_id = hashlib.md5(
                        article.get("url", "").encode()
                    ).hexdigest()

                    if article_id not in seen:
                        message = {
                            "id": article_id,
                            "source": "newsapi",
                            "text": article.get("title", "") + " " + 
                                   (article.get("description") or ""),
                            "author": article.get("author", ""),
                            "topic": topic,
                            "url": article.get("url", ""),
                            "created_at": article.get("publishedAt", "")
                        }
                        producer.send(TOPIC, value=message)
                        print(f"Sent [{topic}]: {message['text'][:60]}")
                        seen.add(article_id)

            except Exception as e:
                print(f"Error fetching {topic}: {e}")

        print("Sleeping 15 minutes...")
        time.sleep(900)  # 15 min — protects free tier quota

if __name__ == "__main__":
    run()