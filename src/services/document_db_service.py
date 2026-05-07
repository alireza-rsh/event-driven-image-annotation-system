from messaging.events import make_event, generate_timestamp
from messaging.topics import INFERENCE_COMPLETED, ANNOTATION_STORED


class DocumentDBService:
    def __init__(self, broker, store):
        self.broker = broker
        self.store = store

    def start(self) -> None:
        self.broker.subscribe(INFERENCE_COMPLETED, self.handle_inference_completed)

    def handle_inference_completed(self, event: dict) -> None:
        payload = event["payload"]

        image_id = payload["image_id"]
        image_path = payload["image_path"]
        annotated_path = payload.get("annotated_path")
        objects = payload.get("objects", [])
        status = payload.get("status", "unknown")
        error = payload.get("error")

        now = generate_timestamp()

        document = {
            "image_id": image_id,
            "image_path": image_path,
            "annotated_path": annotated_path,
            "status": status,
            "objects": objects,
            "object_count": len(objects),
            "error": error,
            "created_at": now,
            "updated_at": now,
            "history": [
                "image.submitted",
                "inference.completed",
            ],
        }

        self.store.upsert_image_document(document)

        if status != "success":
            return

        stored_event = make_event(
            topic=ANNOTATION_STORED,
            producer="document_db_service",
            payload={
                "image_id": image_id,
                "document_id": image_id,
                "stored_at": now,
                "object_count": len(objects),
            },
        )

        self.broker.publish(stored_event.topic, stored_event.to_dict())