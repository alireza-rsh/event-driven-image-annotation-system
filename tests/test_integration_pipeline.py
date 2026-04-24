import json
import time

from event_generator import EventGenerator
from messaging.topics import QUERY_COMPLETED
from services.cli_service import CLIService
from services.document_db_service import DocumentDBService
from services.embedding_service import EmbeddingService
from services.inference_service import InferenceService
from services.query_service import QueryService
from conftest import wait_until


def start_all_services(broker):
    services = [
        InferenceService(broker),
        DocumentDBService(broker),
        EmbeddingService(broker),
        QueryService(broker),
    ]

    for service in services:
        service.start()

    time.sleep(0.5)

    return services


def test_full_image_processing_pipeline_from_image_submitted_to_vector_created(broker):
    start_all_services(broker)

    generator = EventGenerator(seed=530)
    image_event = generator.image_submitted(
        image_id="img_1",
        path="images/car_1.jpg",
        source="user",
    )

    broker.publish(image_event["topic"], image_event)

    assert wait_until(lambda: broker.client.hexists("documents", "img_1"))
    assert wait_until(lambda: broker.client.hexists("vectors", "img_1"))

    document = json.loads(broker.client.hget("documents", "img_1"))
    vector = json.loads(broker.client.hget("vectors", "img_1"))

    assert document["image_id"] == "img_1"
    assert document["objects"][0]["label"] == "car"

    assert vector["image_id"] == "img_1"
    assert vector["labels"] == ["car"]


def test_full_query_pipeline_returns_processed_image(broker):
    completed_events = []

    broker.subscribe(QUERY_COMPLETED, lambda event: completed_events.append(event))

    start_all_services(broker)

    generator = EventGenerator(seed=530)

    image_event = generator.image_submitted(
        image_id="img_1",
        path="images/car_1.jpg",
        source="user",
    )

    broker.publish(image_event["topic"], image_event)

    assert wait_until(lambda: broker.client.hexists("vectors", "img_1"))

    query_event = generator.text_query(
        query_id="qry_1",
        text="car",
        top_k=1,
    )

    broker.publish(query_event["topic"], query_event)

    assert wait_until(lambda: len(completed_events) == 1)

    result = completed_events[0]["payload"]["results"][0]

    assert result["image_id"] == "img_1"
    assert result["labels"] == ["car"]
    assert "score" in result


def test_duplicate_image_submission_does_not_duplicate_documents_or_vectors(broker):
    start_all_services(broker)

    generator = EventGenerator(seed=530)

    image_event = generator.image_submitted(
        image_id="img_dup",
        path="images/car_dup.jpg",
        source="user",
    )

    broker.publish(image_event["topic"], image_event)
    broker.publish(image_event["topic"], image_event)
    broker.publish(image_event["topic"], image_event)

    assert wait_until(lambda: broker.client.hlen("documents") == 1)
    assert wait_until(lambda: broker.client.hlen("vectors") == 1)

    assert broker.client.hlen("documents") == 1
    assert broker.client.hlen("vectors") == 1


def test_multiple_images_are_processed_end_to_end(broker):
    start_all_services(broker)

    generator = EventGenerator(seed=530)

    images = [
        ("img_car", "images/car_1.jpg"),
        ("img_person", "images/person_1.jpg"),
        ("img_bike", "images/bike_1.jpg"),
    ]

    for image_id, path in images:
        event = generator.image_submitted(
            image_id=image_id,
            path=path,
            source="user",
        )
        broker.publish(event["topic"], event)

    assert wait_until(lambda: broker.client.hlen("documents") == 3)
    assert wait_until(lambda: broker.client.hlen("vectors") == 3)

    assert broker.client.hexists("documents", "img_car")
    assert broker.client.hexists("documents", "img_person")
    assert broker.client.hexists("documents", "img_bike")


def test_cli_can_submit_image_and_query_end_to_end(broker):
    completed_events = []

    broker.subscribe(QUERY_COMPLETED, lambda event: completed_events.append(event))

    start_all_services(broker)

    cli = CLIService(broker)

    cli.submit_image(
        image_id="img_1",
        path="images/car_1.jpg",
        source="user",
    )

    assert wait_until(lambda: broker.client.hexists("vectors", "img_1"))

    cli.submit_query(
        query_id="qry_1",
        query_text="car",
        top_k=1,
    )

    assert wait_until(lambda: len(completed_events) == 1)

    assert completed_events[0]["payload"]["query_id"] == "qry_1"
    assert completed_events[0]["payload"]["results"][0]["image_id"] == "img_1"