from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, INFERENCE_COMPLETED


class InferenceService:
    def __init__(self, broker, pipeline=None):
        self.broker = broker
        self.pipeline = pipeline

    def start(self):
        self.broker.subscribe(IMAGE_SUBMITTED, self.handle_image_submitted)

    def handle_image_submitted(self, event):
        image_id = event["payload"]["image_id"]
        image_path = event["payload"]["path"]

        try:
            if self.pipeline is None:
                objects = [
                    {
                        "object_id": "obj_001",
                        "label": "car",
                        "bbox": [12, 44, 188, 200],
                        "confidence": 0.93,
                    }
                ]
                annotated_path = None
            else:
                result = self.pipeline.index_image(image_path, image_id=image_id)
                objects = result["detections"]
                annotated_path = result["annotated_path"]

            result_event = make_event(
                topic=INFERENCE_COMPLETED,
                producer="inference_service",
                payload={
                    "image_id": image_id,
                    "image_path": image_path,
                    "annotated_path": annotated_path,
                    "objects": objects,
                    "status": "success",
                },
            )

        except Exception as exc:
            result_event = make_event(
                topic=INFERENCE_COMPLETED,
                producer="inference_service",
                payload={
                    "image_id": image_id,
                    "image_path": image_path,
                    "annotated_path": None,
                    "objects": [],
                    "status": "failed",
                    "error": str(exc),
                },
            )

        self.broker.publish(result_event.topic, result_event.to_dict())