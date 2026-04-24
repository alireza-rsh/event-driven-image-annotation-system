from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from messaging.topics import (
    ALL_TOPICS,
    ANNOTATION_STORED,
    EMBEDDING_CREATED,
    IMAGE_SUBMITTED,
    INFERENCE_COMPLETED,
    QUERY_COMPLETED,
    QUERY_SUBMITTED,
)


@dataclass(frozen=True)
class BaseEvent:
    type: str
    topic: str
    event_id: str
    timestamp: str
    producer: str
    schema_version: str
    payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(
    topic: str,
    producer: str,
    payload: Dict[str, Any],
    event_id: Optional[str] = None,
    timestamp: Optional[str] = None,
    schema_version: str = "1.0",
) -> BaseEvent:
    event = BaseEvent(
        type="publish",
        topic=topic,
        event_id=event_id or f"evt_{uuid4().hex}",
        timestamp=timestamp or utc_now(),
        producer=producer,
        schema_version=schema_version,
        payload=payload,
    )
    validate_event_dict(event.to_dict())
    validate_topic_payload(event.to_dict())
    return event


def validate_event_dict(event: Dict[str, Any]) -> None:
    required_fields = {"type", "topic", "event_id", "timestamp", "producer", "schema_version", "payload"}
    missing = required_fields - set(event.keys())
    if missing:
        raise ValueError(f"Missing required fields: {sorted(missing)}")
    if event["type"] != "publish":
        raise ValueError("type must be 'publish'")
    if event["topic"] not in ALL_TOPICS:
        raise ValueError(f"Unknown topic: {event['topic']}")
    if not event["event_id"]:
        raise ValueError("event_id must not be empty")
    if not event["producer"]:
        raise ValueError("producer must not be empty")
    if not isinstance(event["payload"], dict):
        raise ValueError("payload must be a dictionary")


def _require(payload: Dict[str, Any], topic: str, fields: tuple[str, ...]) -> None:
    for field in fields:
        if field not in payload:
            raise ValueError(f"{topic} missing payload field: {field}")


def validate_topic_payload(event: Dict[str, Any]) -> None:
    topic = event["topic"]
    payload = event["payload"]

    if topic == IMAGE_SUBMITTED:
        _require(payload, topic, ("image_id", "path", "source"))
    elif topic == INFERENCE_COMPLETED:
        _require(payload, topic, ("image_id", "objects", "status"))
        if not isinstance(payload["objects"], list):
            raise ValueError("inference.completed objects must be a list")
    elif topic == ANNOTATION_STORED:
        _require(payload, topic, ("image_id", "document_id", "stored_at", "objects"))
    elif topic == EMBEDDING_CREATED:
        _require(payload, topic, ("image_id", "document_id", "embedding_id", "vector", "vector_dimension"))
    elif topic == QUERY_SUBMITTED:
        _require(payload, topic, ("query_id", "query_type", "top_k"))
        if payload["query_type"] == "text":
            _require(payload, topic, ("query_text",))
        elif payload["query_type"] == "image":
            _require(payload, topic, ("reference_image_id",))
        else:
            raise ValueError("query_type must be 'text' or 'image'")
    elif topic == QUERY_COMPLETED:
        _require(payload, topic, ("query_id", "status", "results"))
