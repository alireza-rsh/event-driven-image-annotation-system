from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict
import uuid

from messaging.topics import (
    ALL_TOPICS,
    IMAGE_SUBMITTED,
    INFERENCE_COMPLETED,
    ANNOTATION_STORED,
    ANNOTATION_CORRECTED,
    EMBEDDING_CREATED,
    QUERY_SUBMITTED,
    QUERY_COMPLETED,
    IMAGE_INDEXED,
)


@dataclass
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


def generate_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_event_id() -> str:
    return f"evt_{uuid.uuid4().hex}"


def make_event(
    topic: str,
    producer: str,
    payload: Dict[str, Any],
    event_type: str = "publish",
    schema_version: str = "1.0",
    event_id: str | None = None,
    timestamp: str | None = None,
) -> BaseEvent:
    return BaseEvent(
        type=event_type,
        topic=topic,
        event_id=event_id or generate_event_id(),
        timestamp=timestamp or generate_timestamp(),
        producer=producer,
        schema_version=schema_version,
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

    if topic == IMAGE_SUBMITTED:
        for field in ("image_id", "path", "source"):
            if field not in payload:
                raise ValueError(f"{IMAGE_SUBMITTED} missing payload field: {field}")

    elif topic == INFERENCE_COMPLETED:
        for field in ("image_id", "image_path", "objects", "status"):
            if field not in payload:
                raise ValueError(f"{INFERENCE_COMPLETED} missing payload field: {field}")

    elif topic == IMAGE_INDEXED:
        for field in ("image_id", "image_path", "annotated_path", "status"):
            if field not in payload:
                raise ValueError(f"{IMAGE_INDEXED} missing payload field: {field}")

    elif topic == ANNOTATION_STORED:
        for field in ("image_id", "document_id", "stored_at"):
            if field not in payload:
                raise ValueError(f"{ANNOTATION_STORED} missing payload field: {field}")

    elif topic == ANNOTATION_CORRECTED:
        for field in ("image_id", "document_id", "changes", "corrected_by"):
            if field not in payload:
                raise ValueError(f"{ANNOTATION_CORRECTED} missing payload field: {field}")

    elif topic == EMBEDDING_CREATED:
        for field in ("image_id", "document_id", "embedding_id", "vector_dimension"):
            if field not in payload:
                raise ValueError(f"{EMBEDDING_CREATED} missing payload field: {field}")

    elif topic == QUERY_SUBMITTED:
        for field in ("query_id", "query_type", "top_k"):
            if field not in payload:
                raise ValueError(f"{QUERY_SUBMITTED} missing payload field: {field}")

        query_type = payload["query_type"]
        if query_type == "text" and "query_text" not in payload:
            raise ValueError("text query requires query_text")
        if query_type == "image" and "query_image_path" not in payload:
            raise ValueError("image query requires query_image_path")

    elif topic == QUERY_COMPLETED:
        for field in ("query_id", "status", "results"):
            if field not in payload:
                raise ValueError(f"{QUERY_COMPLETED} missing payload field: {field}")