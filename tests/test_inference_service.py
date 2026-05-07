from services.inference_service import InferenceService
from messaging.topics import IMAGE_INDEXED, INFERENCE_COMPLETED


class RecordingPipeline:
    def __init__(self):
        self.calls = []

    def index_image(self, image_path, image_id=None):
        self.calls.append((image_path, image_id))
        return {
            "image_id": image_id,
            "image_path": image_path,
            "annotated_path": f"artifacts/{image_id}_annotated.jpg",
            "detections": [
                {
                    "object_id": "obj_0001",
                    "label": "horse",
                    "confidence": 0.95,
                    "bbox": [1, 2, 3, 4],
                }
            ],
            "indexed_items": 2,
            "faiss_total": 10,
        }


def test_inference_service_mock_mode_publishes_success_and_indexed(fake_broker):
    service = InferenceService(fake_broker, pipeline=None)

    service.handle_image_submitted(
        {
            "payload": {
                "image_id": "img_1",
                "path": "images/a.jpg",
            }
        }
    )

    assert [topic for topic, _ in fake_broker.published] == [
        INFERENCE_COMPLETED,
        IMAGE_INDEXED,
    ]

    inference_event = fake_broker.published[0][1]
    indexed_event = fake_broker.published[1][1]

    assert inference_event["payload"]["status"] == "success"
    assert inference_event["payload"]["objects"][0]["label"] == "car"
    assert indexed_event["payload"]["status"] == "searchable"


def test_inference_service_real_mode_uses_pipeline_and_keeps_image_id(fake_broker):
    pipeline = RecordingPipeline()
    service = InferenceService(fake_broker, pipeline=pipeline)

    service.handle_image_submitted(
        {
            "payload": {
                "image_id": "img_77",
                "path": "/tmp/a.jpg",
            }
        }
    )

    assert pipeline.calls == [("/tmp/a.jpg", "img_77")]

    inference_event = fake_broker.published[0][1]
    indexed_event = fake_broker.published[1][1]

    assert inference_event["payload"]["image_id"] == "img_77"
    assert inference_event["payload"]["objects"][0]["label"] == "horse"
    assert indexed_event["payload"]["faiss_total"] == 10


def test_inference_service_publishes_failed_event_on_error(fake_broker):
    class BrokenPipeline:
        def index_image(self, image_path, image_id=None):
            raise RuntimeError("index failed")

    service = InferenceService(fake_broker, pipeline=BrokenPipeline())

    service.handle_image_submitted(
        {
            "payload": {
                "image_id": "img_1",
                "path": "images/a.jpg",
            }
        }
    )

    assert len(fake_broker.published) == 1
    topic, event = fake_broker.published[0]
    assert topic == INFERENCE_COMPLETED
    assert event["payload"]["status"] == "failed"
    assert "index failed" in event["payload"]["error"]


def test_enqueue_image_submitted_uses_thread_pool(fake_broker, monkeypatch):
    service = InferenceService(fake_broker, pipeline=None)
    captured = {}

    def immediate_submit(fn, event):
        captured["called"] = True
        fn(event)

    monkeypatch.setattr(service.pool, "submit", immediate_submit)

    service.enqueue_image_submitted(
        {
            "payload": {
                "image_id": "img_1",
                "path": "images/a.jpg",
            }
        }
    )

    assert captured["called"] is True
    assert fake_broker.published[0][0] == INFERENCE_COMPLETED
