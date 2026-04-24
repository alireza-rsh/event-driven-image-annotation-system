from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from uuid import uuid4

from messaging.topics import ALL_TOPICS


@dataclass
class Event:
    type: str
    topic: str
    event_id: str
    timestamp: str
    producer: str
    schema_version: str
    payload: dict

    def to_dict(self) -> dict:
        return asdict(self)


def make_event(topic: str, producer: str, payload: dict, event_id: str | None = None) -> Event:
    event = Event(
        type="publish",
        topic=topic,
        event_id=event_id or f"evt_{uuid4().hex}",
        timestamp=datetime.now(timezone.utc).isoformat(),
        producer=producer,
        schema_version="1.0",
        payload=payload,
    )
    validate_event_dict(event.to_dict())
    return event


def validate_event_dict(event: dict) -> None:
    required = ["type", "topic", "event_id", "timestamp", "producer", "schema_version", "payload"]

    for field in required:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

    if event["type"] != "publish":
        raise ValueError("Event type must be publish")

    if event["topic"] not in ALL_TOPICS:
        raise ValueError(f"Unknown topic: {event['topic']}")

    if not isinstance(event["payload"], dict):
        raise ValueError("Payload must be a dictionary")