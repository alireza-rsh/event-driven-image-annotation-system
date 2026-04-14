import json
from typing import Callable

import redis

from messaging.broker_interface import BrokerInterface


class RedisBroker(BrokerInterface):
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.redis_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def publish(self, topic: str, event: dict) -> None:
        self.redis_client.publish(topic, json.dumps(event))

    def subscribe(self, topic: str, handler: Callable[[dict], None]) -> None:
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(topic)

        for message in pubsub.listen():
            if message["type"] != "message":
                continue
            event = json.loads(message["data"])
            handler(event)