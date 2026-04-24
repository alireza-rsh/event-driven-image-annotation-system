import random

from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, QUERY_SUBMITTED


class EventGenerator:
    def __init__(self, seed: int = 530):
        self.random = random.Random(seed)

    def image_submitted(self, image_id: str, path: str, source: str = "generator") -> dict:
        return make_event(
            topic=IMAGE_SUBMITTED,
            producer="event_generator",
            event_id=f"evt_{image_id}_submitted",
            payload={
                "image_id": image_id,
                "path": path,
                "source": source,
            },
        ).to_dict()

    def text_query(self, query_id: str, text: str, top_k: int = 3) -> dict:
        return make_event(
            topic=QUERY_SUBMITTED,
            producer="event_generator",
            event_id=f"evt_{query_id}_submitted",
            payload={
                "query_id": query_id,
                "query_type": "text",
                "query_text": text,
                "top_k": top_k,
            },
        ).to_dict()

    def generate_images(self, count: int, image_dir: str = "images"):
        labels = ["car", "person", "bike", "bus", "street"]

        for i in range(count):
            label = self.random.choice(labels)
            image_id = f"img_{i:04d}_{label}"
            path = f"{image_dir}/{image_id}.jpg"
            yield self.image_submitted(image_id=image_id, path=path)