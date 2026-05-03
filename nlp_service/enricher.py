import json
import os
from dotenv import load_dotenv

load_dotenv()

# Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
INPUT_TOPICS = ["raw.hackernews", "raw.newsapi", "raw.youtube"]
OUTPUT_TOPIC = "enriched.nlp"

# Models are created lazily so importing this module during tests doesn't
# trigger heavy downloads / model loads. Tests can pass mock `sentiment_model`
# and `nlp_model` into `enrich()`.
_sentiment_model = None
_nlp_model = None

def get_sentiment_model():
    global _sentiment_model
    if _sentiment_model is None:
        from transformers import pipeline
        _sentiment_model = pipeline("sentiment-analysis",
                                   model="distilbert-base-uncased-finetuned-sst-2-english")
    return _sentiment_model

def get_nlp_model():
    global _nlp_model
    if _nlp_model is None:
        import spacy
        _nlp_model = spacy.load("en_core_web_sm")
    return _nlp_model

def enrich(message, sentiment_model=None, nlp_model=None):
    text = message.get("text", "")[:512]  # truncate for model
    if not text.strip():
        return None

    sentiment_model = sentiment_model or get_sentiment_model()
    nlp_model = nlp_model or get_nlp_model()

    # Sentiment
    try:
        result = sentiment_model(text)[0]
        sentiment = result["label"].lower()
        confidence = round(result.get("score", 0.0), 4)
    except Exception:
        sentiment = "unknown"
        confidence = 0.0

    # Named Entity Recognition
    doc = nlp_model(text)
    entities = [{"text": ent.text, "label": ent.label_}
                for ent in getattr(doc, "ents", [])
                if ent.label_ in ("PERSON", "ORG", "GPE")]

    return {
        **message,
        "sentiment": sentiment,
        "confidence": confidence,
        "entities": entities,
        "enriched": True
    }


def _run_consumer_loop():
    # Import heavy dependencies only when actually running the service
    from kafka import KafkaConsumer, KafkaProducer

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

    print("Starting NLP enrichment service...")
    for msg in consumer:
        enriched = enrich(msg.value)
        if enriched:
            producer.send(OUTPUT_TOPIC, value=enriched)
            print(f"[{enriched.get('source','?')}] {enriched['sentiment']} ({enriched['confidence']}) — {enriched.get('text','')[:60]}")


if __name__ == "__main__":
    _run_consumer_loop()