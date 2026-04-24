import hashlib
import json

from messaging.events import make_event
from messaging.topics import ANNOTATION_STORED, EMBEDDING_CREATED


class EmbeddingService:
    def __init__(self, broker):
        self.broker = broker
        self.redis = broker.client
        self.vector_key = "vectors"

    def start(self):
        self.broker.subscribe(ANNOTATION_STORED, self.handle_annotation_stored)

    def handle_annotation_stored(self, event: dict):
        payload = event["payload"]
        image_id = payload["image_id"]

        if self.redis.hexists(self.vector_key, image_id):
            return

        labels = [obj["label"] for obj in payload["objects"]]
        embedding = self.create_embedding(" ".join(labels))

        vector_record = {
            "image_id": image_id,
            "labels": labels,
            "embedding": embedding,
        }

        self.redis.hset(self.vector_key, image_id, json.dumps(vector_record))

        output_event = make_event(
            topic=EMBEDDING_CREATED,
            producer="embedding_service",
            event_id=f"evt_{image_id}_embedding_created",
            payload={
                "image_id": image_id,
                "embedding_id": image_id,
            },
        ).to_dict()

        self.broker.publish(EMBEDDING_CREATED, output_event)

    def create_embedding(self, text: str, size: int = 8) -> list[float]:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        return [digest[i] / 255.0 for i in range(size)]