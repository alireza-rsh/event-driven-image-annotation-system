from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, INFERENCE_COMPLETED


class InferenceService:
    def __init__(self, broker):
        self.broker = broker
        self.processed_event_ids = set()

    def start(self):
        self.broker.subscribe(IMAGE_SUBMITTED, self.handle_image_submitted)

    def handle_image_submitted(self, event: dict):
        if event["event_id"] in self.processed_event_ids:
            return

        self.processed_event_ids.add(event["event_id"])

        payload = event["payload"]
        image_id = payload["image_id"]
        path = payload["path"]

        objects = self.simulate_inference(path)

        output_event = make_event(
            topic=INFERENCE_COMPLETED,
            producer="inference_service",
            event_id=f"evt_{image_id}_inference_completed",
            payload={
                "image_id": image_id,
                "path": path,
                "objects": objects,
                "model_version": "simulated-v1",
            },
        ).to_dict()

        self.broker.publish(INFERENCE_COMPLETED, output_event)

    def simulate_inference(self, path: str) -> list[dict]:
        lower_path = path.lower()

        if "car" in lower_path:
            label = "car"
        elif "person" in lower_path:
            label = "person"
        elif "bike" in lower_path:
            label = "bike"
        elif "bus" in lower_path:
            label = "bus"
        else:
            label = "object"

        return [
            {
                "label": label,
                "bbox": [10, 20, 100, 160],
                "confidence": 0.91,
            }
        ]