import pytest

from messaging.events import (
    generate_event_id,
    generate_timestamp,
    make_event,
    validate_event_dict,
    validate_topic_payload,
)
from messaging.topics import (
    IMAGE_SUBMITTED,
    IMAGE_INDEXED,
    QUERY_SUBMITTED,
    QUERY_COMPLETED,
)


def test_make_event_auto_generates_required_fields():
    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test_service",
        payload={"image_id": "img_1", "path": "images/a.jpg", "source": "user"},
    ).to_dict()

    validate_event_dict(event)
    assert event["type"] == "publish"
    assert event["event_id"].startswith("evt_")
    assert event["producer"] == "test_service"
    assert event["schema_version"] == "1.0"


def test_generate_helpers_return_non_empty_values():
    assert generate_event_id().startswith("evt_")
    assert "T" in generate_timestamp()


def test_validate_event_rejects_unknown_topic():
    with pytest.raises(ValueError):
        validate_event_dict(
            {
                "type": "publish",
                "topic": "bad.topic",
                "event_id": "evt_1",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "producer": "test",
                "schema_version": "1.0",
                "payload": {},
            }
        )


def test_validate_event_rejects_non_dict_payload():
    with pytest.raises(ValueError):
        validate_event_dict(
            {
                "type": "publish",
                "topic": IMAGE_SUBMITTED,
                "event_id": "evt_1",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "producer": "test",
                "schema_version": "1.0",
                "payload": "bad",
            }
        )


def test_validate_topic_payload_accepts_text_query():
    event = make_event(
        topic=QUERY_SUBMITTED,
        producer="cli_service",
        payload={
            "query_id": "qry_1",
            "query_type": "text",
            "query_text": "horse",
            "top_k": 3,
        },
    ).to_dict()

    validate_event_dict(event)
    validate_topic_payload(event)


def test_validate_topic_payload_rejects_image_query_without_path():
    event = make_event(
        topic=QUERY_SUBMITTED,
        producer="cli_service",
        payload={
            "query_id": "qry_1",
            "query_type": "image",
            "top_k": 3,
        },
    ).to_dict()

    validate_event_dict(event)
    with pytest.raises(ValueError):
        validate_topic_payload(event)


def test_validate_topic_payload_accepts_image_indexed():
    event = make_event(
        topic=IMAGE_INDEXED,
        producer="inference_service",
        payload={
            "image_id": "img_1",
            "image_path": "images/a.jpg",
            "annotated_path": "artifacts/a.jpg",
            "status": "searchable",
            "faiss_total": 5,
        },
    ).to_dict()

    validate_event_dict(event)
    validate_topic_payload(event)


def test_validate_topic_payload_accepts_query_completed():
    event = make_event(
        topic=QUERY_COMPLETED,
        producer="query_service",
        payload={
            "query_id": "qry_1",
            "status": "success",
            "results": [{"image_id": "img_1", "score": 0.9}],
        },
    ).to_dict()

    validate_event_dict(event)
    validate_topic_payload(event)
