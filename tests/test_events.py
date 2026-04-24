import pytest

from event_generator import EventGenerator
from messaging.events import make_event, validate_event_dict
from messaging.topics import (
    IMAGE_SUBMITTED,
    QUERY_SUBMITTED,
    INFERENCE_COMPLETED,
)


def test_make_event_creates_valid_event():
    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test_service",
        event_id="evt_test_1",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "source": "user",
        },
    ).to_dict()

    validate_event_dict(event)

    assert event["type"] == "publish"
    assert event["topic"] == IMAGE_SUBMITTED
    assert event["event_id"] == "evt_test_1"
    assert event["producer"] == "test_service"
    assert event["schema_version"] == "1.0"
    assert event["payload"]["image_id"] == "img_1"


def test_make_event_auto_generates_event_id():
    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test_service",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "source": "user",
        },
    ).to_dict()

    assert event["event_id"].startswith("evt_")
    validate_event_dict(event)


@pytest.mark.parametrize(
    "missing_field",
    [
        "type",
        "topic",
        "event_id",
        "timestamp",
        "producer",
        "schema_version",
        "payload",
    ],
)
def test_validate_event_rejects_missing_required_fields(missing_field):
    event = {
        "type": "publish",
        "topic": IMAGE_SUBMITTED,
        "event_id": "evt_1",
        "timestamp": "2026-04-01T00:00:00Z",
        "producer": "test",
        "schema_version": "1.0",
        "payload": {},
    }

    event.pop(missing_field)

    with pytest.raises(ValueError):
        validate_event_dict(event)


def test_validate_event_rejects_unknown_topic():
    event = {
        "type": "publish",
        "topic": "unknown.topic",
        "event_id": "evt_1",
        "timestamp": "2026-04-01T00:00:00Z",
        "producer": "test",
        "schema_version": "1.0",
        "payload": {},
    }

    with pytest.raises(ValueError):
        validate_event_dict(event)


def test_validate_event_rejects_non_publish_type():
    event = {
        "type": "subscribe",
        "topic": IMAGE_SUBMITTED,
        "event_id": "evt_1",
        "timestamp": "2026-04-01T00:00:00Z",
        "producer": "test",
        "schema_version": "1.0",
        "payload": {},
    }

    with pytest.raises(ValueError):
        validate_event_dict(event)


def test_validate_event_rejects_non_dict_payload():
    event = {
        "type": "publish",
        "topic": IMAGE_SUBMITTED,
        "event_id": "evt_1",
        "timestamp": "2026-04-01T00:00:00Z",
        "producer": "test",
        "schema_version": "1.0",
        "payload": "bad-payload",
    }

    with pytest.raises(ValueError):
        validate_event_dict(event)


def test_event_generator_creates_valid_image_event():
    generator = EventGenerator(seed=530)

    event = generator.image_submitted(
        image_id="img_1",
        path="images/car_1.jpg",
        source="user",
    )

    validate_event_dict(event)

    assert event["topic"] == IMAGE_SUBMITTED
    assert event["payload"]["image_id"] == "img_1"
    assert event["payload"]["path"] == "images/car_1.jpg"
    assert event["payload"]["source"] == "user"


def test_event_generator_creates_valid_query_event():
    generator = EventGenerator(seed=530)

    event = generator.text_query(
        query_id="qry_1",
        text="car",
        top_k=5,
    )

    validate_event_dict(event)

    assert event["topic"] == QUERY_SUBMITTED
    assert event["payload"]["query_id"] == "qry_1"
    assert event["payload"]["query_text"] == "car"
    assert event["payload"]["top_k"] == 5


def test_event_generator_is_deterministic_for_same_seed():
    first = list(EventGenerator(seed=10).generate_images(5))
    second = list(EventGenerator(seed=10).generate_images(5))

    assert [event["payload"] for event in first] == [
        event["payload"] for event in second
    ]


def test_make_event_rejects_bad_topic_immediately():
    with pytest.raises(ValueError):
        make_event(
            topic="bad.topic",
            producer="test",
            payload={},
        )


def test_valid_inference_completed_event_shape():
    event = make_event(
        topic=INFERENCE_COMPLETED,
        producer="inference_service",
        event_id="evt_img_1_inference_completed",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "objects": [
                {
                    "label": "car",
                    "bbox": [10, 20, 100, 160],
                    "confidence": 0.91,
                }
            ],
            "model_version": "simulated-v1",
        },
    ).to_dict()

    validate_event_dict(event)

    assert event["payload"]["objects"][0]["label"] == "car"