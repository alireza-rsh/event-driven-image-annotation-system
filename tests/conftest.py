import pytest

from messaging.redis_broker import RedisBroker
from tests.helpers import InMemoryDocumentStore


class FakeBroker:
    def __init__(self):
        self.published = []
        self.subscribers = {}

    def publish(self, topic, event):
        self.published.append((topic, event))

    def subscribe(self, topic, handler):
        self.subscribers[topic] = handler

    def ping(self):
        return True


@pytest.fixture
def fake_broker():
    return FakeBroker()


@pytest.fixture
def fake_store():
    return InMemoryDocumentStore()


@pytest.fixture
def broker():
    try:
        b = RedisBroker()
        b.ping()
    except Exception as exc:
        pytest.skip(f"Redis is not available for integration tests: {exc}")

    try:
        b.redis_client.flushdb()
    except Exception:
        pass

    yield b

    try:
        b.redis_client.flushdb()
    except Exception:
        pass


@pytest.fixture
def sample_image_event():
    return {
        "type": "publish",
        "topic": "image.submitted",
        "event_id": "evt_test_001",
        "timestamp": "2026-01-01T00:00:00Z",
        "producer": "test_suite",
        "schema_version": "1.0",
        "payload": {
            "image_id": "img_test_001",
            "path": "/tmp/test.jpg",
            "source": "test",
        },
    }