import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = "raw.hackernews"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_new_stories():
    url = "https://hacker-news.firebaseio.com/v0/newstories.json"
    response = requests.get(url)
    return response.json()[:30]  # top 30 newest stories

def fetch_story(story_id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
    response = requests.get(url)
    return response.json()

def run():
    seen = set()
    print("Starting Hacker News producer...")
    while True:
        story_ids = fetch_new_stories()
        for story_id in story_ids:
            if story_id not in seen:
                story = fetch_story(story_id)
                if story and story.get("title"):
                    message = {
                        "id": str(story.get("id")),
                        "source": "hackernews",
                        "text": story.get("title", ""),
                        "url": story.get("url", ""),
                        "score": story.get("score", 0),
                        "author": story.get("by", ""),
                        "created_at": story.get("time", 0)
                    }
                    producer.send(TOPIC, value=message)
                    print(f"Sent: {message['text'][:60]}")
                    seen.add(story_id)
        time.sleep(30)  # poll every 30 seconds

if __name__ == "__main__":
    run()