from pathlib import Path
from threading import Thread
import uuid

from messaging.events import make_event
from messaging.topics import IMAGE_SUBMITTED, QUERY_SUBMITTED, QUERY_COMPLETED


class CLIService:
    def __init__(self, broker):
        self.broker = broker
        self.listener_thread = None

    def start(self) -> None:
        """
        Start listening for query results in a background thread.
        This keeps the CLI responsive while Redis subscribe() blocks.
        """
        if self.listener_thread and self.listener_thread.is_alive():
            return

        self.listener_thread = Thread(
            target=self._listen_for_query_completed,
            daemon=True,
        )
        self.listener_thread.start()

    def _listen_for_query_completed(self) -> None:
        self.broker.subscribe(QUERY_COMPLETED, self.handle_query_completed)

    def submit_image(self, path: str, source: str = "user_upload", image_id: str | None = None) -> str:
        """
        Simulate image upload by publishing image metadata.
        Returns the generated image_id so the user can query by it later.
        """
        image_path = Path(path)

        if not image_path.exists():
            raise FileNotFoundError(f"Image path does not exist: {image_path}")

        final_image_id = image_id or f"img_{uuid.uuid4().hex[:12]}"

        event = make_event(
            topic=IMAGE_SUBMITTED,
            producer="cli_service",
            payload={
                "image_id": final_image_id,
                "path": str(image_path),
                "source": source,
            },
        )

        self.broker.publish(event.topic, event.to_dict())
        print(f"Uploaded image: image_id={final_image_id}, path={image_path}")
        return final_image_id

    def submit_text_query(self, query_text: str, top_k: int = 5, query_id: str | None = None) -> str:
        """
        Search by natural language, for example:
        'red car'
        'person near crosswalk'
        """
        if not query_text or not query_text.strip():
            raise ValueError("query_text must not be empty")

        final_query_id = query_id or f"qry_{uuid.uuid4().hex[:12]}"

        event = make_event(
            topic=QUERY_SUBMITTED,
            producer="cli_service",
            payload={
                "query_id": final_query_id,
                "query_type": "text",
                "query_text": query_text.strip(),
                "top_k": top_k,
            },
        )

        self.broker.publish(event.topic, event.to_dict())
        print(f"Submitted text query: query_id={final_query_id}, text='{query_text}', top_k={top_k}")
        return final_query_id

    def submit_image_query(self, reference_image_id: str, top_k: int = 5, query_id: str | None = None) -> str:
        """
        Search by image, meaning:
        'give me the top-k similar images to this uploaded image'
        """
        if not reference_image_id or not reference_image_id.strip():
            raise ValueError("reference_image_id must not be empty")

        final_query_id = query_id or f"qry_{uuid.uuid4().hex[:12]}"

        event = make_event(
            topic=QUERY_SUBMITTED,
            producer="cli_service",
            payload={
                "query_id": final_query_id,
                "query_type": "image",
                "reference_image_id": reference_image_id.strip(),
                "top_k": top_k,
            },
        )

        self.broker.publish(event.topic, event.to_dict())
        print(
            f"Submitted image query: query_id={final_query_id}, "
            f"reference_image_id={reference_image_id}, top_k={top_k}"
        )
        return final_query_id

    def handle_query_completed(self, event: dict) -> None:
        payload = event["payload"]
        query_id = payload.get("query_id", "unknown")
        status = payload.get("status", "unknown")
        results = payload.get("results", [])

        print("\n=== Query Completed ===")
        print(f"query_id: {query_id}")
        print(f"status: {status}")
        print(f"result_count: {len(results)}")

        for i, result in enumerate(results, start=1):
            image_id = result.get("image_id", "unknown")
            score = result.get("score", "n/a")
            print(f"{i}. image_id={image_id}, score={score}")

        print("=======================\n")

    def run_interactive(self) -> None:
        """
        Minimal interactive CLI for demo/testing.
        """
        self.start()

        print("CLI started.")
        print("Commands:")
        print("  upload")
        print("  search-text")
        print("  search-image")
        print("  exit")

        while True:
            command = input("> ").strip().lower()

            if command == "upload":
                path = input("Image path: ").strip()
                source = input("Source [default=user_upload]: ").strip() or "user_upload"

                try:
                    image_id = self.submit_image(path=path, source=source)
                    print(f"Use this image_id later for image search: {image_id}")
                except Exception as exc:
                    print(f"Upload failed: {exc}")

            elif command == "search-text":
                query_text = input("What do you want to find? ").strip()
                top_k_raw = input("top_k [default=5]: ").strip()

                try:
                    top_k = int(top_k_raw) if top_k_raw else 5
                    self.submit_text_query(query_text=query_text, top_k=top_k)
                except Exception as exc:
                    print(f"Text query failed: {exc}")

            elif command == "search-image":
                reference_image_id = input("Reference image_id: ").strip()
                top_k_raw = input("top_k [default=5]: ").strip()

                try:
                    top_k = int(top_k_raw) if top_k_raw else 5
                    self.submit_image_query(reference_image_id=reference_image_id, top_k=top_k)
                except Exception as exc:
                    print(f"Image query failed: {exc}")

            elif command in {"exit", "quit"}:
                print("Bye.")
                break

            else:
                print("Unknown command.")