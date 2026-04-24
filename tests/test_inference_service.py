from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, INFERENCE_COMPLETED
from services.inference_service import InferenceService
from conftest import wait_until


def test_simulate_inference_detects_car_from_path(broker):
    service = InferenceService(broker)

    objects = service.simulate_inference("images/car_001.jpg")

    assert len(objects) == 1
    assert objects[0]["label"] == "car"
    assert objects[0]["bbox"] == [10, 20, 100, 160]
    assert objects[0]["confidence"] == 0.91


def test_simulate_inference_detects_person_from_path(broker):
    service = InferenceService(broker)

    objects = service.simulate_inference("images/person_001.jpg")

    assert objects[0]["label"] == "person"


def test_simulate_inference_detects_bike_from_path(broker):
    service = InferenceService(broker)

    objects = service.simulate_inference("images/bike_001.jpg")

    assert objects[0]["label"] == "bike"


def test_simulate_inference_detects_bus_from_path(broker):
    service = InferenceService(broker)

    objects = service.simulate_inference("images/bus_001.jpg")

    assert objects[0]["label"] == "bus"


def test_simulate_inference_defaults_to_object(broker):
    service = InferenceService(broker)

    objects = service.simulate_inference("images/unknown_001.jpg")

    assert objects[0]["label"] == "object"


def test_inference_service_publishes_inference_completed(broker):
    received = []

    broker.subscribe(INFERENCE_COMPLETED, lambda event: received.append(event))

    service = InferenceService(broker)
    service.start()

    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_img_1_submitted",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "source": "user",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, event)

    assert wait_until(lambda: len(received) == 1)

    output = received[0]
    assert output["topic"] == INFERENCE_COMPLETED
    assert output["producer"] == "inference_service"
    assert output["payload"]["image_id"] == "img_1"
    assert output["payload"]["objects"][0]["label"] == "car"
    assert output["payload"]["model_version"] == "simulated-v1"


def test_inference_service_is_idempotent_for_same_event(broker):
    received = []

    broker.subscribe(INFERENCE_COMPLETED, lambda event: received.append(event))

    service = InferenceService(broker)
    service.start()

    event = make_event(
        topic=IMAGE_SUBMITTED,
        producer="test",
        event_id="evt_same_event",
        payload={
            "image_id": "img_1",
            "path": "images/car_1.jpg",
            "source": "user",
        },
    ).to_dict()

    broker.publish(IMAGE_SUBMITTED, event)
    broker.publish(IMAGE_SUBMITTED, event)

    assert wait_until(lambda: len(received) == 1)