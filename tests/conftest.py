import time

import pytest

from messaging.redis_broker import RedisBroker


TEST_REDIS_URL = "redis://localhost:6379/1"


@pytest.fixture()
def broker():
    redis_broker = RedisBroker(TEST_REDIS_URL)

    try:
        redis_broker.ping()
    except Exception:
        pytest.skip("Redis is not running on localhost:6379")

    redis_broker.client.flushdb()

    yield redis_broker

    redis_broker.client.flushdb()
    redis_broker.close()


def wait_until(condition, timeout=5, interval=0.05):
    start = time.time()

    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)

    return False