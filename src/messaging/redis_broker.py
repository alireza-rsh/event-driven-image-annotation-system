import json
import threading
from typing import Callable

import redis

from messaging.events import validate_event_dict


class RedisBroker:
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.client = redis.Redis.from_url(self.redis_url, decode_responses=True)
        self.threads: list[threading.Thread] = []

    def publish(self, topic: str, event: dict) -> None:
        validate_event_dict(event)
        self.client.publish(topic, json.dumps(event))

    def subscribe(self, topic: str, handler: Callable[[dict], None]) -> None:
        pubsub = self.client.pubsub()
        pubsub.subscribe(topic)

        def listen():
            for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                try:
                    event = json.loads(message["data"])
                    validate_event_dict(event)
                    handler(event)
                except Exception as exc:
                    print(f"[RedisBroker] Failed to handle message on {topic}: {exc}")

        thread = threading.Thread(target=listen, daemon=True)
        thread.start()
        self.threads.append(thread)

    def ping(self) -> bool:
        return self.client.ping()