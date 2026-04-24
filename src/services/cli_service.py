from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, QUERY_SUBMITTED, QUERY_COMPLETED


class CLIService:
    def __init__(self, broker):
        self.broker = broker

    def start(self):
        self.broker.subscribe(QUERY_COMPLETED, self.handle_query_completed)

    def submit_image(self, image_id: str, path: str, source: str = "cli"):
        event = make_event(
            topic=IMAGE_SUBMITTED,
            producer="cli_service",
            event_id=f"evt_{image_id}_submitted",
            payload={
                "image_id": image_id,
                "path": path,
                "source": source,
            },
        ).to_dict()

        self.broker.publish(IMAGE_SUBMITTED, event)

    def submit_query(self, query_id: str, query_text: str, top_k: int = 3):
        event = make_event(
            topic=QUERY_SUBMITTED,
            producer="cli_service",
            event_id=f"evt_{query_id}_submitted",
            payload={
                "query_id": query_id,
                "query_type": "text",
                "query_text": query_text,
                "top_k": top_k,
            },
        ).to_dict()

        self.broker.publish(QUERY_SUBMITTED, event)

    def handle_query_completed(self, event: dict):
        print("Query completed:")
        print(event["payload"])