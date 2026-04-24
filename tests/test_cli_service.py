from messaging.topics import IMAGE_SUBMITTED, QUERY_SUBMITTED
from services.cli_service import CLIService
from conftest import wait_until


def test_cli_service_publishes_image_submitted(broker):
    received = []
    broker.subscribe(IMAGE_SUBMITTED, lambda event: received.append(event))

    cli = CLIService(broker)

    cli.submit_image(
        image_id="img_1",
        path="images/car_1.jpg",
        source="user",
    )

    assert wait_until(lambda: len(received) == 1)

    event = received[0]
    assert event["topic"] == IMAGE_SUBMITTED
    assert event["producer"] == "cli_service"
    assert event["payload"]["image_id"] == "img_1"
    assert event["payload"]["path"] == "images/car_1.jpg"
    assert event["payload"]["source"] == "user"


def test_cli_service_publishes_query_submitted(broker):
    received = []
    broker.subscribe(QUERY_SUBMITTED, lambda event: received.append(event))

    cli = CLIService(broker)

    cli.submit_query(
        query_id="qry_1",
        query_text="car",
        top_k=2,
    )

    assert wait_until(lambda: len(received) == 1)

    event = received[0]
    assert event["topic"] == QUERY_SUBMITTED
    assert event["producer"] == "cli_service"
    assert event["payload"]["query_id"] == "qry_1"
    assert event["payload"]["query_text"] == "car"
    assert event["payload"]["top_k"] == 2