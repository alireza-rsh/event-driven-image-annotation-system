from dataclasses import dataclass
from typing import Any, Dict

from messaging.topics import ALL_TOPICS


@dataclass
class BaseEvent:
    type: str
    topic: str
    event_id: str
    timestamp: str
    producer: str
    schema_version: str
    payload: Dict[str, Any]


def make_event(topic: str, event_id: str, producer: str, payload: Dict[str, Any], timestamp: str) -> BaseEvent:
    return BaseEvent(
        type="publish",
        topic=topic,
        event_id=event_id,
        timestamp=timestamp,
        producer=producer,
        schema_version="1.0",
        payload=payload,
    )

def validate_event_dict(event: Dict[str, Any]) -> None:
    required_fields = {
        "type",
        "topic",
        "event_id",
        "timestamp",
        "producer",
        "schema_version",
        "payload",
    }

    missing = required_fields - set(event.keys())
    if missing:
        raise ValueError(f"Missing required fields: {sorted(missing)}")

    if event["topic"] not in ALL_TOPICS:
        raise ValueError(f"Unknown topic: {event['topic']}")

    if not isinstance(event["payload"], dict):
        raise ValueError("payload must be a dictionary")

    if not event["event_id"]:
        raise ValueError("event_id must not be empty")

def validate_topic_payload(event: Dict[str, Any]) -> None:
    topic = event["topic"]
    payload = event["payload"]

    if topic == "image.submitted":
        for field in ("image_id", "path", "source"):
            if field not in payload:
                raise ValueError(f"image.submitted missing payload field: {field}")

    elif topic == "inference.completed":
        for field in ("image_id", "objects", "status"):
            if field not in payload:
                raise ValueError(f"inference.completed missing payload field: {field}")

    elif topic == "annotation.stored":
        for field in ("image_id", "document_id", "stored_at"):
            if field not in payload:
                raise ValueError(f"annotation.stored missing payload field: {field}")

    elif topic == "embedding.created":
        for field in ("image_id", "document_id", "embedding_id", "vector_dimension"):
            if field not in payload:
                raise ValueError(f"embedding.created missing payload field: {field}")

    elif topic == "query.submitted":
        for field in ("query_id", "query_type", "top_k"):
            if field not in payload:
                raise ValueError(f"query.submitted missing payload field: {field}")

    elif topic == "query.completed":
        for field in ("query_id", "status", "results"):
            if field not in payload:
                raise ValueError(f"query.completed missing payload field: {field}")