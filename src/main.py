import argparse
import threading
import time
from uuid import uuid4

from event_generator import EventGenerator
from messaging.redis_broker import RedisBroker
from retrieval.real_pipeline import RealImageRetrievalSystem
from services.cli_service import CLIService
from services.document_db_service import DocumentDBService
from services.embedding_service import EmbeddingService
from services.inference_service import InferenceService
from services.query_service import QueryService


def _start_in_thread(service):
    thread = threading.Thread(target=service.start, daemon=True)
    thread.start()
    return thread


def start_mock_services(broker):
    services = [
        InferenceService(broker),
        DocumentDBService(broker),
        EmbeddingService(broker),
        QueryService(broker),
    ]

    threads = []
    for service in services:
        threads.append(_start_in_thread(service))

    return services, threads


def start_real_services(broker, pipeline):
    """
    In real mode, the pipeline handles:
    - detection
    - box creation
    - embeddings
    - FAISS indexing/search

    So we only start the services that actually use it.
    """
    services = [
        InferenceService(broker, pipeline=pipeline),
        QueryService(broker, pipeline=pipeline),
    ]

    threads = []
    for service in services:
        threads.append(_start_in_thread(service))

    return services, threads


def run_demo():
    broker = RedisBroker()
    broker.ping()

    start_mock_services(broker)

    cli = CLIService(broker)
    cli.start()

    time.sleep(1)

    generator = EventGenerator(seed=530)

    for event in generator.generate_images(3):
        broker.publish(event["topic"], event)

    time.sleep(2)

    query_event = generator.text_query("qry_001", "car", top_k=2)
    broker.publish(query_event["topic"], query_event)

    time.sleep(2)


def run_interactive():
    """
    Current mock interactive mode.
    """
    broker = RedisBroker()
    broker.ping()

    start_mock_services(broker)

    cli = CLIService(broker)
    cli.start()

    print("Mock system started.")
    print("1. Upload image")
    print("2. Submit text query")
    print("3. Exit")

    while True:
        choice = input("\nChoose an option: ").strip()

        if choice == "1":
            path = input("Image path: ").strip()

            if not path:
                print("Image path cannot be empty.")
                continue

            image_id = f"img_{uuid4().hex[:8]}"

            cli.submit_image(
                image_id=image_id,
                path=path,
                source="user",
            )

            print(f"Published image.submitted for image_id={image_id}")

        elif choice == "2":
            query_text = input("Query text: ").strip()

            if not query_text:
                print("Query text cannot be empty.")
                continue

            top_k_input = input("Top K [default=3]: ").strip()

            try:
                top_k = int(top_k_input) if top_k_input else 3
            except ValueError:
                print("Top K must be a number.")
                continue

            query_id = f"qry_{uuid4().hex[:8]}"

            cli.submit_query(
                query_id=query_id,
                query_type="text",
                query_text=query_text,
                top_k=top_k,
            )

            print(f"Published query.submitted for query_id={query_id}")

        elif choice == "3":
            print("Exiting.")
            break

        else:
            print("Invalid option. Choose 1, 2, or 3.")

        time.sleep(1)


def run_real():
    """
    Real mode:
    - upload real images
    - detect real objects
    - create real embeddings
    - search with FAISS
    """
    broker = RedisBroker()
    broker.ping()

    pipeline = RealImageRetrievalSystem()
    start_real_services(broker, pipeline)

    cli = CLIService(broker)
    cli.start()

    print("Real system started.")
    print("1. Upload image and index it")
    print("2. Search by text")
    print("3. Search by image")
    print("4. Exit")

    while True:
        choice = input("\nChoose an option: ").strip()

        if choice == "1":
            path = input("Image path: ").strip()

            if not path:
                print("Image path cannot be empty.")
                continue

            image_id = f"img_{uuid4().hex[:8]}"

            try:
                cli.submit_image(
                    image_id=image_id,
                    path=path,
                    source="user",
                )
                print(f"Published image.submitted for image_id={image_id}")
            except Exception as exc:
                print(f"Upload/indexing failed: {exc}")

        elif choice == "2":
            query_text = input("What do you want to find? ").strip()

            if not query_text:
                print("Query text cannot be empty.")
                continue

            top_k_input = input("Top K [default=3]: ").strip()

            try:
                top_k = int(top_k_input) if top_k_input else 3
            except ValueError:
                print("Top K must be a number.")
                continue

            query_id = f"qry_{uuid4().hex[:8]}"

            try:
                cli.submit_query(
                    query_id=query_id,
                    query_type="text",
                    query_text=query_text,
                    top_k=top_k,
                )
                print(f"Published text query for query_id={query_id}")
            except Exception as exc:
                print(f"Text search failed: {exc}")

        elif choice == "3":
            query_image_path = input("Query image path: ").strip()

            if not query_image_path:
                print("Query image path cannot be empty.")
                continue

            top_k_input = input("Top K [default=3]: ").strip()

            try:
                top_k = int(top_k_input) if top_k_input else 3
            except ValueError:
                print("Top K must be a number.")
                continue

            query_id = f"qry_{uuid4().hex[:8]}"

            try:
                cli.submit_query(
                    query_id=query_id,
                    query_type="image",
                    query_image_path=query_image_path,
                    top_k=top_k,
                )
                print(f"Published image query for query_id={query_id}")
            except Exception as exc:
                print(f"Image search failed: {exc}")

        elif choice == "4":
            print("Exiting.")
            break

        else:
            print("Invalid option. Choose 1, 2, 3, or 4.")

        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["demo", "interactive", "real"],
        required=True,
    )
    args = parser.parse_args()

    if args.mode == "demo":
        run_demo()
    elif args.mode == "interactive":
        run_interactive()
    elif args.mode == "real":
        run_real()


if __name__ == "__main__":
    main()