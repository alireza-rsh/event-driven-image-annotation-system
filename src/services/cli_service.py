from threading import Thread

from messaging.events import make_event
from messaging.topics import (
    IMAGE_SUBMITTED,
    QUERY_SUBMITTED,
    QUERY_COMPLETED,
    IMAGE_INDEXED,
)


class CLIService:
    def __init__(self, broker):
        self.broker = broker
        self.query_thread = None
        self.index_thread = None

    def start(self):
        if self.query_thread is None or not self.query_thread.is_alive():
            self.query_thread = Thread(
                target=self._listen_for_query_completed,
                daemon=True,
            )
            self.query_thread.start()

        if self.index_thread is None or not self.index_thread.is_alive():
            self.index_thread = Thread(
                target=self._listen_for_image_indexed,
                daemon=True,
            )
            self.index_thread.start()

    def _listen_for_query_completed(self):
        self.broker.subscribe(QUERY_COMPLETED, self.handle_query_completed)

    def _listen_for_image_indexed(self):
        self.broker.subscribe(IMAGE_INDEXED, self.handle_image_indexed)

    def submit_image(self, image_id, path, source):
        event = make_event(
            topic=IMAGE_SUBMITTED,
            producer="cli_service",
            payload={
                "image_id": image_id,
                "path": path,
                "source": source,
            },
        )
        self.broker.publish(event.topic, event.to_dict())

    def submit_query(self, query_id, query_type, top_k, query_text=None, query_image_path=None):
        payload = {
            "query_id": query_id,
            "query_type": query_type,
            "top_k": top_k,
        }

        if query_type == "text":
            if not query_text:
                raise ValueError("query_text is required for text query")
            payload["query_text"] = query_text

        elif query_type == "image":
            if not query_image_path:
                raise ValueError("query_image_path is required for image query")
            payload["query_image_path"] = query_image_path

        else:
            raise ValueError(f"Unsupported query_type: {query_type}")

        event = make_event(
            topic=QUERY_SUBMITTED,
            producer="cli_service",
            payload=payload,
        )
        self.broker.publish(event.topic, event.to_dict())

    def handle_image_indexed(self, event):
        payload = event["payload"]
        print(
            f"\n[READY] image_id={payload.get('image_id')} is searchable "
            f"(faiss_total={payload.get('faiss_total')})\n"
        )

    def handle_query_completed(self, event):
        payload = event["payload"]

        print("\n=== Query completed ===")
        print(f"query_id: {payload.get('query_id')}")
        print(f"status: {payload.get('status')}")

        if payload.get("status") == "failed":
            print(f"error: {payload.get('error')}")
            print("=======================\n")
            return

        results = payload.get("results", [])
        if not results:
            print("No results found.")
        else:
            for i, result in enumerate(results, start=1):
                print(
                    f"{i}. image_id={result.get('image_id')} "
                    f"score={result.get('score')} "
                    f"label={result.get('label')}"
                )

        print("=======================\n")