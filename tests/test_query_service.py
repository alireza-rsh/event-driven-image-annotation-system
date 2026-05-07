from services.query_service import QueryService
from messaging.topics import QUERY_COMPLETED


class RecordingPipeline:
    def __init__(self):
        self.calls = []

    def search_by_text(self, text, k=5):
        self.calls.append(("text", text, k))
        return [{"image_id": "img_text", "score": 0.7, "label": "horse"}]

    def search_by_image(self, image_path, k=5):
        self.calls.append(("image", image_path, k))
        return [{"image_id": "img_image", "score": 0.8, "label": "tv"}]


def test_query_service_mock_mode_returns_topk(fake_broker):
    service = QueryService(fake_broker, pipeline=None)

    service.handle_query_submitted(
        {
            "payload": {
                "query_id": "qry_1",
                "query_type": "text",
                "query_text": "car",
                "top_k": 2,
            }
        }
    )

    topic, event = fake_broker.published[-1]
    assert topic == QUERY_COMPLETED
    assert event["payload"]["status"] == "success"
    assert len(event["payload"]["results"]) == 2


def test_query_service_uses_text_pipeline(fake_broker):
    pipeline = RecordingPipeline()
    service = QueryService(fake_broker, pipeline=pipeline)

    service.handle_query_submitted(
        {
            "payload": {
                "query_id": "qry_1",
                "query_type": "text",
                "query_text": "horse",
                "top_k": 3,
            }
        }
    )

    assert pipeline.calls == [("text", "horse", 3)]
    _, event = fake_broker.published[-1]
    assert event["payload"]["results"][0]["label"] == "horse"


def test_query_service_uses_image_pipeline(fake_broker):
    pipeline = RecordingPipeline()
    service = QueryService(fake_broker, pipeline=pipeline)

    service.handle_query_submitted(
        {
            "payload": {
                "query_id": "qry_1",
                "query_type": "image",
                "query_image_path": "images/query.jpg",
                "top_k": 4,
            }
        }
    )

    assert pipeline.calls == [("image", "images/query.jpg", 4)]
    _, event = fake_broker.published[-1]
    assert event["payload"]["results"][0]["label"] == "tv"


def test_query_service_returns_failed_for_missing_text(fake_broker):
    pipeline = RecordingPipeline()
    service = QueryService(fake_broker, pipeline=pipeline)

    service.handle_query_submitted(
        {
            "payload": {
                "query_id": "qry_1",
                "query_type": "text",
                "top_k": 3,
            }
        }
    )

    _, event = fake_broker.published[-1]
    assert event["payload"]["status"] == "failed"
    assert "query_text" in event["payload"]["error"]


def test_query_service_returns_failed_on_pipeline_error(fake_broker):
    class BrokenPipeline:
        def search_by_text(self, text, k=5):
            raise RuntimeError("boom")

    service = QueryService(fake_broker, pipeline=BrokenPipeline())

    service.handle_query_submitted(
        {
            "payload": {
                "query_id": "qry_1",
                "query_type": "text",
                "query_text": "horse",
                "top_k": 3,
            }
        }
    )

    _, event = fake_broker.published[-1]
    assert event["payload"]["status"] == "failed"
    assert "boom" in event["payload"]["error"]
