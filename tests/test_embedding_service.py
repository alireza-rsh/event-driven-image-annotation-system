import json

from messaging.events import make_event
from messaging.topics import ANNOTATION_STORED, EMBEDDING_CREATED
from services.embedding_service import EmbeddingService
from conftest import wait_until


def make_annotation_stored_event(image_id="img_1", label="car"):
    return make_event(
        topic=ANNOTATION_STORED,
        producer="document_db_service",
        event_id=f"evt_{image_id}_annotation_stored",
        payload={
            "image_id": image_id,
            "document_id": image_id,
            "objects": [
                {
                    "label": label,
                    "bbox": [10, 20, 100, 160],
                    "confidence": 0.91,
                }
            ],
        },
    ).to_dict()


def test_create_embedding_is_deterministic(broker):
    service = EmbeddingService(broker)

    first = service.create_embedding("car")
    second = service.create_embedding("car")

    assert first == second


def test_create_embedding_has_expected_size(broker):
    service = EmbeddingService(broker)

    embedding = service.create_embedding("car", size=8)

    assert len(embedding) == 8
    assert all(isinstance(value, float) for value in embedding)


def test_embedding_service_stores_vector(broker):
    service = EmbeddingService(broker)
    service.start()

    event = make_annotation_stored_event("img_1", "car")
    broker.publish(ANNOTATION_STORED, event)

    assert wait_until(lambda: broker.client.hexists("vectors", "img_1"))

    raw = broker.client.hget("vectors", "img_1")
    vector = json.loads(raw)

    assert vector["image_id"] == "img_1"
    assert vector["labels"] == ["car"]
    assert len(vector["embedding"]) == 8


def test_embedding_service_publishes_embedding_created(broker):
    received = []

    broker.subscribe(EMBEDDING_CREATED, lambda event: received.append(event))

    service = EmbeddingService(broker)
    service.start()

    event = make_annotation_stored_event("img_1", "car")
    broker.publish(ANNOTATION_STORED, event)

    assert wait_until(lambda: len(received) == 1)

    output = received[0]
    assert output["topic"] == EMBEDDING_CREATED
    assert output["producer"] == "embedding_service"
    assert output["payload"]["image_id"] == "img_1"
    assert output["payload"]["embedding_id"] == "img_1"


def test_embedding_service_is_idempotent(broker):
    service = EmbeddingService(broker)
    service.start()

    event = make_annotation_stored_event("img_1", "car")

    broker.publish(ANNOTATION_STORED, event)
    broker.publish(ANNOTATION_STORED, event)

    assert wait_until(lambda: broker.client.hlen("vectors") == 1)