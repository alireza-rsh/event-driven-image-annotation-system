import json

from messaging.events import make_event
from messaging.topics import INFERENCE_COMPLETED, ANNOTATION_STORED
from services.document_db_service import DocumentDBService
from conftest import wait_until


def make_inference_completed_event(image_id="img_1"):
    return make_event(
        topic=INFERENCE_COMPLETED,
        producer="inference_service",
        event_id=f"evt_{image_id}_inference_completed",
        payload={
            "image_id": image_id,
            "path": f"images/{image_id}_car.jpg",
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


def test_document_db_service_stores_document(broker):
    service = DocumentDBService(broker)
    service.start()

    event = make_inference_completed_event("img_1")
    broker.publish(INFERENCE_COMPLETED, event)

    assert wait_until(lambda: broker.client.hexists("documents", "img_1"))

    raw = broker.client.hget("documents", "img_1")
    document = json.loads(raw)

    assert document["image_id"] == "img_1"
    assert document["path"] == "images/img_1_car.jpg"
    assert document["objects"][0]["label"] == "car"
    assert document["model_version"] == "simulated-v1"
    assert document["review"]["status"] == "not_reviewed"


def test_document_db_service_publishes_annotation_stored(broker):
    received = []

    broker.subscribe(ANNOTATION_STORED, lambda event: received.append(event))

    service = DocumentDBService(broker)
    service.start()

    event = make_inference_completed_event("img_1")
    broker.publish(INFERENCE_COMPLETED, event)

    assert wait_until(lambda: len(received) == 1)

    output = received[0]
    assert output["topic"] == ANNOTATION_STORED
    assert output["producer"] == "document_db_service"
    assert output["payload"]["image_id"] == "img_1"
    assert output["payload"]["document_id"] == "img_1"
    assert output["payload"]["objects"][0]["label"] == "car"


def test_document_db_service_get_document_returns_document(broker):
    service = DocumentDBService(broker)
    service.start()

    event = make_inference_completed_event("img_1")
    broker.publish(INFERENCE_COMPLETED, event)

    assert wait_until(lambda: service.get_document("img_1") is not None)

    document = service.get_document("img_1")
    assert document["image_id"] == "img_1"


def test_document_db_service_get_document_returns_none_for_missing_image(broker):
    service = DocumentDBService(broker)

    assert service.get_document("missing") is None


def test_document_db_service_is_idempotent(broker):
    service = DocumentDBService(broker)
    service.start()

    event = make_inference_completed_event("img_1")

    broker.publish(INFERENCE_COMPLETED, event)
    broker.publish(INFERENCE_COMPLETED, event)

    assert wait_until(lambda: broker.client.hlen("documents") == 1)

    assert broker.client.hlen("documents") == 1