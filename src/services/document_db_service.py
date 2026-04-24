import json

from messaging.events import make_event
from messaging.topics import INFERENCE_COMPLETED, ANNOTATION_STORED


class DocumentDBService:
    def __init__(self, broker):
        self.broker = broker
        self.redis = broker.client
        self.document_key = "documents"

    def start(self):
        self.broker.subscribe(INFERENCE_COMPLETED, self.handle_inference_completed)

    def handle_inference_completed(self, event: dict):
        payload = event["payload"]
        image_id = payload["image_id"]

        if self.redis.hexists(self.document_key, image_id):
            return

        document = {
            "image_id": image_id,
            "path": payload["path"],
            "objects": payload["objects"],
            "model_version": payload["model_version"],
            "history": [
                {
                    "status": INFERENCE_COMPLETED,
                    "event_id": event["event_id"],
                    "timestamp": event["timestamp"],
                    "producer": event["producer"],
                }
            ],
            "review": {
                "status": "not_reviewed",
                "notes": [],
            },
        }

        self.redis.hset(self.document_key, image_id, json.dumps(document))

        output_event = make_event(
            topic=ANNOTATION_STORED,
            producer="document_db_service",
            event_id=f"evt_{image_id}_annotation_stored",
            payload={
                "image_id": image_id,
                "document_id": image_id,
                "objects": payload["objects"],
            },
        ).to_dict()

        self.broker.publish(ANNOTATION_STORED, output_event)
    
    def get_document(self, image_id: str) -> dict | None:
        raw = self.redis.hget(self.document_key, image_id)
        if raw is None:
            return None
        return json.loads(raw)