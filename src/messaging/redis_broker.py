import json
import threading
import time
from typing import Callable

import redis

from messaging.events import validate_event_dict


class RedisBroker:
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.client = redis.Redis.from_url(self.redis_url, decode_responses=True)

        self.threads: list[threading.Thread] = []
        self.pubsubs = []
        self.pubsub_clients = []

        self._closed = threading.Event()

    def publish(self, topic: str, event: dict) -> None:
        validate_event_dict(event)

        if event["topic"] != topic:
            raise ValueError(f"Publish topic mismatch: {topic} != {event['topic']}")

        self.client.publish(topic, json.dumps(event))

    def subscribe(self, topic: str, handler: Callable[[dict], None]) -> None:
        pubsub_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
        pubsub = pubsub_client.pubsub(ignore_subscribe_messages=False)

        self.pubsub_clients.append(pubsub_client)
        self.pubsubs.append(pubsub)

        pubsub.subscribe(topic)

        start = time.time()
        subscribed = False

        while time.time() - start < 3:
            message = pubsub.get_message(timeout=0.1)

            if (
                message is not None
                and message.get("type") == "subscribe"
                and message.get("channel") == topic
            ):
                subscribed = True
                break

        if not subscribed:
            raise RuntimeError(f"Could not subscribe to Redis topic: {topic}")

        def listen():
            while not self._closed.is_set():
                try:
                    message = pubsub.get_message(timeout=0.2)

                    if message is None:
                        continue

                    if message.get("type") != "message":
                        continue

                    event = json.loads(message["data"])
                    validate_event_dict(event)
                    handler(event)

                except ValueError as exc:
                    if self._closed.is_set() or "I/O operation on closed file" in str(exc):
                        break
                    print(f"[RedisBroker] Invalid event on topic {topic}: {exc}")

                except redis.exceptions.ConnectionError:
                    if not self._closed.is_set():
                        print(f"[RedisBroker] Redis connection error on topic {topic}")
                    break

                except Exception as exc:
                    if not self._closed.is_set():
                        print(f"[RedisBroker] Failed to handle message on {topic}: {exc}")

        thread = threading.Thread(target=listen, daemon=True)
        thread.start()
        self.threads.append(thread)

    def ping(self) -> bool:
        return self.client.ping()

    def close(self) -> None:
        self._closed.set()

        for pubsub in self.pubsubs:
            try:
                pubsub.unsubscribe()
            except Exception:
                pass

        time.sleep(0.1)

        for pubsub in self.pubsubs:
            try:
                pubsub.close()
            except Exception:
                pass

        for client in self.pubsub_clients:
            try:
                client.close()
            except Exception:
                pass

        for thread in self.threads:
            thread.join(timeout=1)

        try:
            self.client.close()
        except Exception:
            pass