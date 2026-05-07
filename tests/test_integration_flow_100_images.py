import pytest

from messaging.topics import IMAGE_INDEXED, QUERY_COMPLETED
from services.cli_service import CLIService
from services.document_db_service import DocumentDBService
from services.inference_service import InferenceService
from services.query_service import QueryService
from tests.helpers import InMemoryDocumentStore, wait_until


class FastFakePipeline:
    def __init__(self):
        self.indexed = {}

    def index_image(self, image_path, image_id=None):
        idx = int(image_id.split("_")[-1]) if image_id and image_id.split("_")[-1].isdigit() else len(self.indexed)
        label = "horse" if idx % 2 == 0 else "person"
        self.indexed[image_id] = {
            "image_path": image_path,
            "label": label,
        }
        return {
            "image_id": image_id,
            "image_path": image_path,
            "annotated_path": f"artifacts/{image_id}_annotated.jpg",
            "detections": [
                {
                    "object_id": f"obj_{idx:04d}",
                    "label": label,
                    "confidence": 0.95,
                    "bbox": [1, 2, 10, 20],
                }
            ],
            "indexed_items": 2,
            "faiss_total": len(self.indexed) * 2,
        }

    def search_by_text(self, text, k=5):
        text = text.lower().strip()
        matches = []
        for image_id, item in self.indexed.items():
            if item["label"] == text:
                matches.append(
                    {
                        "image_id": image_id,
                        "score": 0.9,
                        "label": item["label"],
                        "item_type": "object_crop",
                    }
                )
        return matches[:k]

    def search_by_image(self, image_path, k=5):
        results = []
        for image_id, item in self.indexed.items():
            results.append(
                {
                    "image_id": image_id,
                    "score": 0.8,
                    "label": item["label"],
                    "item_type": "full_image",
                }
            )
        return results[:k]


@pytest.mark.integration
def test_full_flow_with_100_images_over_redis(broker):
    pipeline = FastFakePipeline()
    store = InMemoryDocumentStore()

    inference = InferenceService(broker, pipeline=pipeline)
    document_db = DocumentDBService(broker, store=store)
    query = QueryService(broker, pipeline=pipeline)

    inference.start()
    document_db.start()
    query.start()

    ready_events = []
    query_events = []

    broker.subscribe(IMAGE_INDEXED, lambda event: ready_events.append(event))
    broker.subscribe(QUERY_COMPLETED, lambda event: query_events.append(event))

    cli = CLIService(broker)

    total_images = 100
    for i in range(total_images):
        cli.submit_image(
            image_id=f"img_{i}",
            path=f"/tmp/image_{i}.jpg",
            source="bulk_test",
        )

    assert wait_until(lambda: len(ready_events) == total_images, timeout=15)
    assert wait_until(lambda: store.stats()["documents"] == total_images, timeout=15)

    doc = store.get_image_document("img_0")
    assert doc is not None
    assert doc["status"] == "success"
    assert doc["objects"][0]["label"] in {"horse", "person"}

    cli.submit_query(
        query_id="qry_horse",
        query_type="text",
        top_k=10,
        query_text="horse",
    )

    assert wait_until(lambda: len(query_events) >= 1, timeout=5)

    last = query_events[-1]
    assert last["payload"]["status"] == "success"
    assert len(last["payload"]["results"]) > 0
    assert all(result["label"] == "horse" for result in last["payload"]["results"])
