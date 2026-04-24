import hashlib
import json
import math

from messaging.events import make_event
from messaging.topics import QUERY_SUBMITTED, QUERY_COMPLETED


class QueryService:
    def __init__(self, broker):
        self.broker = broker
        self.redis = broker.client
        self.vector_key = "vectors"

    def start(self):
        self.broker.subscribe(QUERY_SUBMITTED, self.handle_query_submitted)

    def handle_query_submitted(self, event: dict):
        payload = event["payload"]

        query_id = payload["query_id"]
        query_text = payload["query_text"]
        top_k = payload.get("top_k", 3)

        query_embedding = self.create_embedding(query_text)

        results = []
        all_vectors = self.redis.hgetall(self.vector_key)

        for image_id, raw_record in all_vectors.items():
            record = json.loads(raw_record)
            score = self.cosine_similarity(query_embedding, record["embedding"])

            results.append(
                {
                    "image_id": image_id,
                    "labels": record["labels"],
                    "score": score,
                }
            )

        results.sort(key=lambda item: item["score"], reverse=True)
        results = results[:top_k]

        output_event = make_event(
            topic=QUERY_COMPLETED,
            producer="query_service",
            event_id=f"evt_{query_id}_completed",
            payload={
                "query_id": query_id,
                "query_text": query_text,
                "results": results,
            },
        ).to_dict()

        self.broker.publish(QUERY_COMPLETED, output_event)

    def create_embedding(self, text: str, size: int = 8) -> list[float]:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        return [digest[i] / 255.0 for i in range(size)]

    def cosine_similarity(self, left: list[float], right: list[float]) -> float:
        dot = sum(a * b for a, b in zip(left, right))
        left_norm = math.sqrt(sum(a * a for a in left))
        right_norm = math.sqrt(sum(b * b for b in right))

        if left_norm == 0 or right_norm == 0:
            return 0.0

        return dot / (left_norm * right_norm)