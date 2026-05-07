from messaging.topics import ANNOTATION_STORED
from services.document_db_service import DocumentDBService


def test_document_db_service_stores_success_document_and_publishes(fake_broker, fake_store):
    service = DocumentDBService(fake_broker, fake_store)

    service.handle_inference_completed(
        {
            "payload": {
                "image_id": "img_1",
                "image_path": "images/a.jpg",
                "annotated_path": "artifacts/a.jpg",
                "objects": [
                    {
                        "object_id": "obj_1",
                        "label": "horse",
                        "confidence": 0.9,
                        "bbox": [1, 2, 3, 4],
                    }
                ],
                "status": "success",
            }
        }
    )

    doc = fake_store.get_image_document("img_1")
    assert doc is not None
    assert doc["image_id"] == "img_1"
    assert doc["image_path"] == "images/a.jpg"
    assert doc["annotated_path"] == "artifacts/a.jpg"
    assert doc["object_count"] == 1
    assert doc["objects"][0]["label"] == "horse"
    assert doc["history"] == ["image.submitted", "inference.completed"]

    topic, event = fake_broker.published[-1]
    assert topic == ANNOTATION_STORED
    assert event["payload"]["image_id"] == "img_1"
    assert event["payload"]["object_count"] == 1


def test_document_db_service_stores_failed_document_but_does_not_publish(fake_broker, fake_store):
    service = DocumentDBService(fake_broker, fake_store)

    service.handle_inference_completed(
        {
            "payload": {
                "image_id": "img_2",
                "image_path": "images/b.jpg",
                "annotated_path": None,
                "objects": [],
                "status": "failed",
                "error": "boom",
            }
        }
    )

    doc = fake_store.get_image_document("img_2")
    assert doc is not None
    assert doc["status"] == "failed"
    assert doc["error"] == "boom"
    assert fake_broker.published == []


def test_document_db_service_updates_store_stats(fake_broker, fake_store):
    service = DocumentDBService(fake_broker, fake_store)

    for i in range(3):
        service.handle_inference_completed(
            {
                "payload": {
                    "image_id": f"img_{i}",
                    "image_path": f"images/{i}.jpg",
                    "annotated_path": f"artifacts/{i}.jpg",
                    "objects": [{"object_id": "o1", "label": "tv", "confidence": 0.8, "bbox": [1,2,3,4]}],
                    "status": "success",
                }
            }
        )

    stats = fake_store.stats()
    assert stats["documents"] == 3
    assert stats["objects"] == 3
