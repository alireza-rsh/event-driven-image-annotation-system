from messaging.events import make_event
from messaging.topics import INFERENCE_COMPLETED, ANNOTATION_STORED
from datetime import datetime, timezone

class DocumentDBService:
    def __init__(self, broker):
        self.broker = broker
    
    def start(self) -> None:
        self.broker.subscribe(INFERENCE_COMPLETED, self.handle_inference_completed)

    def handle_inference_completed(self, event: dict) -> None:
        image_id = event["payload"]["image_id"]
        timestamp = datetime.now(timezone.utc).isoformat()

        stored_event = make_event(
            topic=ANNOTATION_STORED,
            event_id=f"evt_{image_id}_annotation_stored",
            timestamp=timestamp,
            producer="document_db_service",
            payload={
                "image_id": image_id,
                "document_id": f"doc_{image_id}",
                "stored_at": timestamp,
                "object_count": len(event["payload"]["objects"]),
            },
        )
        self.broker.publish(stored_event.topic, stored_event.to_dict())