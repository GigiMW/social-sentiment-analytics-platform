import pytest

from nlp_service import enricher


class _MockEnt:
    def __init__(self, text, label):
        self.text = text
        self.label_ = label


class _MockDoc:
    def __init__(self, ents):
        self.ents = ents


def test_enrich_positive_sentiment_and_entities():
    msg = {"text": "OpenAI announces GPT-4, I love it", "source": "unittest"}

    def mock_sentiment(text):
        return [{"label": "POSITIVE", "score": 0.9876}]

    def mock_nlp(text):
        return _MockDoc([_MockEnt("OpenAI", "ORG")])

    out = enricher.enrich(msg, sentiment_model=mock_sentiment, nlp_model=mock_nlp)
    assert out is not None
    assert out["sentiment"] == "positive"
    assert pytest.approx(out["confidence"], rel=1e-3) == 0.9876
    assert out["enriched"] is True
    assert isinstance(out["entities"], list)
    assert out["entities"][0]["text"] == "OpenAI"


def test_enrich_empty_text_returns_none():
    msg = {"text": "   ", "source": "unittest"}
    assert enricher.enrich(msg, sentiment_model=lambda t: [], nlp_model=lambda t: _MockDoc([])) is None


def test_entity_label_filtering():
    msg = {"text": "Alice visited Paris and met with Bob at Acme Corp", "source": "unittest"}

    def mock_sentiment(text):
        return [{"label": "NEGATIVE", "score": 0.1}]

    def mock_nlp(text):
        return _MockDoc([
            _MockEnt("Alice", "PERSON"),
            _MockEnt("Paris", "GPE"),
            _MockEnt("Bob", "PERSON"),
            _MockEnt("Acme Corp", "ORG"),
            _MockEnt("something", "LOC"),
        ])

    out = enricher.enrich(msg, sentiment_model=mock_sentiment, nlp_model=mock_nlp)
    labels = {e["label"] for e in out["entities"]}
    assert "PERSON" in labels or "person" in labels
    assert "LOC" not in labels
