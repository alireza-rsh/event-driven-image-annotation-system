from concurrent.futures import ThreadPoolExecutor

from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, INFERENCE_COMPLETED, IMAGE_INDEXED


class InferenceService:
    def __init__(self, broker, pipeline=None):
        self.broker = broker
        self.pipeline = pipeline
        self.pool = ThreadPoolExecutor(max_workers=4)

    def start(self):
        self.broker.subscribe(IMAGE_SUBMITTED, self.enqueue_image_submitted)

    def enqueue_image_submitted(self, event):
        self.pool.submit(self.handle_image_submitted, event)

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
                faiss_total = None
            else:
                result = self.pipeline.index_image(image_path, image_id=image_id)
                objects = result["detections"]
                annotated_path = result["annotated_path"]
                faiss_total = result["faiss_total"]

            inference_event = make_event(
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
            self.broker.publish(inference_event.topic, inference_event.to_dict())

            indexed_event = make_event(
                topic=IMAGE_INDEXED,
                producer="inference_service",
                payload={
                    "image_id": image_id,
                    "image_path": image_path,
                    "annotated_path": annotated_path,
                    "status": "searchable",
                    "faiss_total": faiss_total,
                },
            )
            self.broker.publish(indexed_event.topic, indexed_event.to_dict())

        except Exception as exc:
            failed_event = make_event(
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
            self.broker.publish(failed_event.topic, failed_event.to_dict())