from messaging.events import make_event
from messaging.topics import QUERY_SUBMITTED, QUERY_COMPLETED


class QueryService:
    def __init__(self, broker, pipeline=None):
        self.broker = broker
        self.pipeline = pipeline

    def start(self):
        self.broker.subscribe(QUERY_SUBMITTED, self.handle_query_submitted)

    def handle_query_submitted(self, event):
        payload = event["payload"]
        query_id = payload["query_id"]
        query_type = payload["query_type"]
        top_k = payload["top_k"]

        try:
            if self.pipeline is None:
                results = [
                    {"image_id": "img_001", "score": 0.96},
                    {"image_id": "img_002", "score": 0.91},
                    {"image_id": "img_003", "score": 0.88},
                ][:top_k]
            else:
                if query_type == "text":
                    query_text = payload.get("query_text")
                    if not query_text:
                        raise ValueError("query_text is required for text query")
                    results = self.pipeline.search_by_text(query_text, k=top_k)

                elif query_type == "image":
                    query_image_path = payload.get("query_image_path")
                    if not query_image_path:
                        raise ValueError("query_image_path is required for image query")
                    results = self.pipeline.search_by_image(query_image_path, k=top_k)

                else:
                    raise ValueError(f"Unsupported query_type: {query_type}")

            completed_event = make_event(
                topic=QUERY_COMPLETED,
                producer="query_service",
                payload={
                    "query_id": query_id,
                    "status": "success",
                    "results": results,
                },
            )

        except Exception as exc:
            completed_event = make_event(
                topic=QUERY_COMPLETED,
                producer="query_service",
                payload={
                    "query_id": query_id,
                    "status": "failed",
                    "error": str(exc),
                    "results": [],
                },
            )

        self.broker.publish(completed_event.topic, completed_event.to_dict())