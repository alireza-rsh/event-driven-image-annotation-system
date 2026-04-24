import json
import time

from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, INFERENCE_COMPLETED
from services.inference_service import InferenceService
from conftest import wait_until


def test_malformed_raw_redis_message_does_not_crash_subscriber(broker):
    received = []

    def handler(event):
        received.append(event)

    broker.subscribe(IMAGE_SUBMITTED, handler)

    broker.client.publish(IMAGE_SUBMITTED, "not-json")

    good_event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_good_after_bad",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "source": "user",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, good_event)

    assert wait_until(lambda: len(received) == 1)
    assert received[0]["event_id"] == "evt_good_after_bad"


def test_invalid_json_event_is_ignored_without_crashing_listener(broker):
    received = []

    broker.subscribe(IMAGE_SUBMITTED, lambda event: received.append(event))

    bad_event = {
        "type": "publish",
        "topic": IMAGE_SUBMITTED,
        "event_id": "evt_bad",
        "timestamp": "2026-04-01T00:00:00Z",
        "producer": "test",
    }

    broker.client.publish(IMAGE_SUBMITTED, json.dumps(bad_event))

    good_event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_good",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "source": "user",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, good_event)

    assert wait_until(lambda: len(received) == 1)
    assert received[0]["event_id"] == "evt_good"


def test_delayed_subscriber_only_receives_future_messages_with_redis_pubsub(broker):
    event_before_subscribe = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_before_subscribe",
        payload={
            "image_id": "img_before",
            "path": "images/car_before.jpg",
            "source": "user",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, event_before_subscribe)

    received = []
    broker.subscribe(IMAGE_SUBMITTED, lambda event: received.append(event))

    event_after_subscribe = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_after_subscribe",
        payload={
            "image_id": "img_after",
            "path": "images/car_after.jpg",
            "source": "user",
        },
    ).to_dict()

    time.sleep(0.2)
    broker.publish(IMAGE_SUBMITTED, event_after_subscribe)

    assert wait_until(lambda: len(received) == 1)
    assert received[0]["event_id"] == "evt_after_subscribe"


def test_inference_service_continues_after_bad_message(broker):
    received = []

    broker.subscribe(INFERENCE_COMPLETED, lambda event: received.append(event))

    service = InferenceService(broker)
    service.start()

    broker.client.publish(IMAGE_SUBMITTED, "bad-json-message")

    good_event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_good_image",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "source": "user",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, good_event)

    assert wait_until(lambda: len(received) == 1)
    assert received[0]["payload"]["image_id"] == "img_1"