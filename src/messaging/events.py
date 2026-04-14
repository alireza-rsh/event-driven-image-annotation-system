from dataclasses import dataclass
from typing import Any, Dict


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