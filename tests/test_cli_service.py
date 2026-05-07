import pytest

from messaging.topics import IMAGE_INDEXED, IMAGE_SUBMITTED, QUERY_COMPLETED, QUERY_SUBMITTED
from services.cli_service import CLIService


def test_submit_image_publishes_image_submitted(fake_broker):
    cli = CLIService(fake_broker)

    cli.submit_image("img_1", "images/a.jpg", "user")

    topic, event = fake_broker.published[-1]
    assert topic == IMAGE_SUBMITTED
    assert event["payload"]["image_id"] == "img_1"
    assert event["payload"]["path"] == "images/a.jpg"
    assert event["payload"]["source"] == "user"


def test_submit_text_query_publishes_query_submitted(fake_broker):
    cli = CLIService(fake_broker)

    cli.submit_query(
        query_id="qry_1",
        query_type="text",
        top_k=5,
        query_text="horse",
    )

    topic, event = fake_broker.published[-1]
    assert topic == QUERY_SUBMITTED
    assert event["payload"]["query_id"] == "qry_1"
    assert event["payload"]["query_type"] == "text"
    assert event["payload"]["query_text"] == "horse"
    assert event["payload"]["top_k"] == 5


def test_submit_image_query_publishes_query_submitted(fake_broker):
    cli = CLIService(fake_broker)

    cli.submit_query(
        query_id="qry_2",
        query_type="image",
        top_k=3,
        query_image_path="images/query.jpg",
    )

    topic, event = fake_broker.published[-1]
    assert topic == QUERY_SUBMITTED
    assert event["payload"]["query_type"] == "image"
    assert event["payload"]["query_image_path"] == "images/query.jpg"


def test_submit_query_rejects_missing_text(fake_broker):
    cli = CLIService(fake_broker)

    with pytest.raises(ValueError):
        cli.submit_query("qry_1", "text", 3)


def test_submit_query_rejects_missing_image_path(fake_broker):
    cli = CLIService(fake_broker)

    with pytest.raises(ValueError):
        cli.submit_query("qry_1", "image", 3)


def test_submit_query_rejects_unknown_type(fake_broker):
    cli = CLIService(fake_broker)

    with pytest.raises(ValueError):
        cli.submit_query("qry_1", "audio", 3)


def test_handle_image_indexed_prints_ready(fake_broker, capsys):
    cli = CLIService(fake_broker)

    cli.handle_image_indexed(
        {
            "payload": {
                "image_id": "img_1",
                "faiss_total": 10,
            }
        }
    )

    out = capsys.readouterr().out
    assert "[READY]" in out
    assert "img_1" in out
    assert "10" in out


def test_handle_query_completed_prints_results(fake_broker, capsys):
    cli = CLIService(fake_broker)

    cli.handle_query_completed(
        {
            "payload": {
                "query_id": "qry_1",
                "status": "success",
                "results": [
                    {"image_id": "img_1", "score": 0.9, "label": "horse"},
                    {"image_id": "img_2", "score": 0.8, "label": "person"},
                ],
            }
        }
    )

    out = capsys.readouterr().out
    assert "Query completed" in out
    assert "horse" in out
    assert "img_1" in out


def test_start_registers_both_listeners(fake_broker, monkeypatch):
    from services import cli_service as cli_module

    calls = []

    class DummyThread:
        def __init__(self, target, daemon):
            self.target = target
            self.daemon = daemon
            self._alive = False

        def start(self):
            calls.append(self.target.__name__)
            self._alive = True
            self.target()

        def is_alive(self):
            return False

    monkeypatch.setattr(cli_module, "Thread", DummyThread)

    cli = CLIService(fake_broker)
    cli.start()

    assert QUERY_COMPLETED in fake_broker.subscribers
    assert IMAGE_INDEXED in fake_broker.subscribers
    assert "_listen_for_query_completed" in calls
    assert "_listen_for_image_indexed" in calls
