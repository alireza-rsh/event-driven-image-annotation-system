import time

import pytest

from messaging.events import make_event
from messaging.redis_broker import RedisBroker
from messaging.topics import IMAGE_SUBMITTED
from tests.helpers import wait_until


def test_redis_broker_roundtrip_publish_subscribe(broker):
    received = []

    broker.subscribe(IMAGE_SUBMITTED, lambda event: received.append(event))

    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test_service",
        payload={
            "image_id": "img_1",
            "path": "images/a.jpg",
            "source": "user",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, event)

    assert wait_until(lambda: len(received) == 1)
    assert received[0]["payload"]["image_id"] == "img_1"


def test_redis_broker_rejects_topic_mismatch(broker):
    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test_service",
        payload={
            "image_id": "img_1",
            "path": "images/a.jpg",
            "source": "user",
        },
    ).to_dict()

    with pytest.raises(ValueError):
        broker.publish("query.submitted", event)


def test_redis_broker_rejects_invalid_event_on_publish(broker):
    with pytest.raises(ValueError):
        broker.publish(
            IMAGE_SUBMITTED,
            {
                "topic": IMAGE_SUBMITTED,
                "payload": {},
            },
        )
