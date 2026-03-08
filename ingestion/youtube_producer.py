import json
import time
from googleapiclient.discovery import build
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
TOPIC = "raw.youtube"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# Search terms to track — edit these freely
SEARCH_QUERIES = ["artificial intelligence", "technology news", "stock market"]

def fetch_video_ids(query):
    response = youtube.search().list(
        q=query,
        part="id",
        type="video",
        maxResults=5,  # keep low to save quota
        order="date"
    ).execute()
    return [item["id"]["videoId"] for item in response.get("items", [])]

def fetch_comments(video_id):
    try:
        response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=20,
            order="time"
        ).execute()
        return response.get("items", [])
    except Exception:
        # comments disabled on some videos
        return []

def run():
    seen = set()
    print("Starting YouTube producer...")
    while True:
        for query in SEARCH_QUERIES:
            try:
                video_ids = fetch_video_ids(query)
                for video_id in video_ids:
                    comments = fetch_comments(video_id)
                    for item in comments:
                        comment = item["snippet"]["topLevelComment"]["snippet"]
                        comment_id = item["id"]

                        if comment_id not in seen:
                            message = {
                                "id": comment_id,
                                "source": "youtube",
                                "text": comment.get("textDisplay", ""),
                                "author": comment.get("authorDisplayName", ""),
                                "video_id": video_id,
                                "topic": query,
                                "like_count": comment.get("likeCount", 0),
                                "created_at": comment.get("publishedAt", "")
                            }
                            producer.send(TOPIC, value=message)
                            print(f"Sent: {message['text'][:60]}")
                            seen.add(comment_id)

            except Exception as e:
                print(f"Error on query '{query}': {e}")

        print("Sleeping 60 minutes to protect quota...")
        time.sleep(3600)  # 1 hour between runs

if __name__ == "__main__":
    run()