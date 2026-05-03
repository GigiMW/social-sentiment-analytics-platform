import json
import os
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
import spacy
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
INPUT_TOPICS = ["raw.hackernews", "raw.newsapi", "raw.youtube"]
OUTPUT_TOPIC = "enriched.nlp"

# Load models
print("Loading NLP models...")
sentiment_model = pipeline("sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english")
nlp = spacy.load("en_core_web_sm")
print("Models loaded!")

consumer = KafkaConsumer(
    *INPUT_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="nlp-enricher"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def enrich(message):
    text = message.get("text", "")[:512]  # truncate for model
    if not text.strip():
        return None

    # Sentiment
    try:
        result = sentiment_model(text)[0]
        sentiment = result["label"].lower()
        confidence = round(result["score"], 4)
    except Exception:
        sentiment = "unknown"
        confidence = 0.0

    # Named Entity Recognition
    doc = nlp(text)
    entities = [{"text": ent.text, "label": ent.label_}
                for ent in doc.ents
                if ent.label_ in ("PERSON", "ORG", "GPE")]

    return {
        **message,
        "sentiment": sentiment,
        "confidence": confidence,
        "entities": entities,
        "enriched": True
    }

print("Starting NLP enrichment service...")
for msg in consumer:
    enriched = enrich(msg.value)
    if enriched:
        producer.send(OUTPUT_TOPIC, value=enriched)
        print(f"[{enriched['source']}] {enriched['sentiment']} ({enriched['confidence']}) — {enriched['text'][:60]}")