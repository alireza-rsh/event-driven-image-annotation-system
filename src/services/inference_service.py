from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED,INFERENCE_COMPLETED
from datetime import datetime, timezone

class InferenceService:
    def __init__(self, broker):
        self.broker = broker
    
    def start(self) -> None:
        self.broker.subscribe(IMAGE_SUBMITTED, self.handle_image_submitted)

    def handle_image_submitted(self, event: dict) -> None:
        image_id = event["payload"]["image_id"]
        mock_objects = [
            {
                "object_id": "obj_001",
                "label": "car",
                "bbox": [12, 44, 188, 200],
                "confidence": 0.93,
            }
        ]

        result_event = make_event(
            topic=INFERENCE_COMPLETED,
            event_id=f"evt_{image_id}_inference_completed",
            timestamp=datetime.now(timezone.utc).isoformat(),
            producer="inference_service",
            payload={
                "image_id": image_id,
                "objects": mock_objects,
                "status": "success",
            },
        )
        self.broker.publish(result_event.topic, result_event.to_dict())