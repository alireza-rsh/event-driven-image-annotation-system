from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, QUERY_SUBMITTED, QUERY_COMPLETED
from datetime import datetime, timezone


class CLIService:
    def __init__(self, broker):
        self.broker = broker
    
    def start(self):
        self.broker.subscribe(QUERY_COMPLETED, self.handle_query_completed)

    def submit_image(self, image_id, path, source):
        event = make_event(
            topic=IMAGE_SUBMITTED,
            event_id=f"evt_{image_id}_submitted",
            timestamp=datetime.now(timezone.utc).isoformat(),
            producer="cli_service",
            payload={
                "image_id": image_id,
                "path": path,
                "source": source,
            },
        )
        self.broker.publish(event.topic, event.to_dict())

    def submit_query(self, query_id, query_type, top_k, query_text):
        payload = {
            "query_id": query_id,
            "query_type": query_type,
            "top_k": top_k,
        }
        if query_text is not None:
            payload["query_text"] = query_text

        event = make_event(
            topic=QUERY_SUBMITTED,
            event_id=f"evt_{query_id}_submitted",
            timestamp=datetime.now(timezone.utc).isoformat(),
            producer="cli_service",
            payload=payload,
        )
        self.broker.publish(event.topic, event.to_dict())

    def handle_query_completed(self, event):
        print("Query completed:", event)