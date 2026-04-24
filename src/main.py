import argparse
import time
from uuid import uuid4

from event_generator import EventGenerator
from messaging.redis_broker import RedisBroker
from services.cli_service import CLIService
from services.document_db_service import DocumentDBService
from services.embedding_service import EmbeddingService
from services.inference_service import InferenceService
from services.query_service import QueryService


def start_services(broker):
    services = [
        InferenceService(broker),
        DocumentDBService(broker),
        EmbeddingService(broker),
        QueryService(broker),
    ]

    for service in services:
        service.start()

    return services


def run_demo():
    broker = RedisBroker()
    broker.ping()

    start_services(broker)

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
    broker = RedisBroker()
    broker.ping()

    start_services(broker)

    cli = CLIService(broker)
    cli.start()

    print("System started.")
    print("1. Upload image")
    print("2. Submit query")
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--interactive", action="store_true")
    args = parser.parse_args()

    if args.demo:
        run_demo()
    elif args.interactive:
        run_interactive()
    else:
        print("Run one of these:")
        print("PYTHONPATH=src python src/main.py --demo")
        print("PYTHONPATH=src python src/main.py --interactive")


if __name__ == "__main__":
    main()