import pytest

from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED
from conftest import wait_until


def test_redis_ping_works(broker):
    assert broker.ping() is True


def test_publish_and_subscribe_receives_message(broker):
    received = []

    def handler(event):
        received.append(event)

    broker.subscribe(IMAGE_SUBMITTED, handler)

    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_broker_test",
        payload={
            "image_id": "img_1",
            "path": "images/car.jpg",
            "source": "test",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, event)

    assert wait_until(lambda: len(received) == 1)
    assert received[0]["event_id"] == "evt_broker_test"


def test_publish_rejects_invalid_event_before_sending(broker):
    bad_event = {
        "type": "publish",
        "topic": "bad.topic",
        "event_id": "evt_bad",
        "timestamp": "2026-04-01T00:00:00Z",
        "producer": "test",
        "schema_version": "1.0",
        "payload": {},
    }

    with pytest.raises(ValueError):
        broker.publish("bad.topic", bad_event)


def test_multiple_subscribers_receive_same_event(broker):
    first_received = []
    second_received = []

    broker.subscribe(IMAGE_SUBMITTED, lambda event: first_received.append(event))
    broker.subscribe(IMAGE_SUBMITTED, lambda event: second_received.append(event))

    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_multi_subscriber",
        payload={
            "image_id": "img_1",
            "path": "images/car.jpg",
            "source": "test",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, event)

    assert wait_until(lambda: len(first_received) == 1)
    assert wait_until(lambda: len(second_received) == 1)


def test_handler_exception_does_not_crash_broker_listener(broker):
    received = []

    def bad_handler(event):
        raise RuntimeError("intentional test failure")

    def good_handler(event):
        received.append(event)

    broker.subscribe(IMAGE_SUBMITTED, bad_handler)
    broker.subscribe(IMAGE_SUBMITTED, good_handler)

    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_handler_error",
        payload={
            "image_id": "img_1",
            "path": "images/car.jpg",
            "source": "test",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, event)

    assert wait_until(lambda: len(received) == 1)