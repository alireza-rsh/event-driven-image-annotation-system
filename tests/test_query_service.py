import json

from messaging.events import make_event
from messaging.topics import QUERY_SUBMITTED, QUERY_COMPLETED
from services.query_service import QueryService
from conftest import wait_until


def insert_vector(broker, image_id, labels, embedding):
    broker.client.hset(
        "vectors",
        image_id,
        json.dumps(
            {
                "image_id": image_id,
                "labels": labels,
                "embedding": embedding,
            }
        ),
    )


def test_cosine_similarity_identical_vectors_is_one(broker):
    service = QueryService(broker)

    score = service.cosine_similarity([1.0, 0.0], [1.0, 0.0])

    assert score == 1.0


def test_cosine_similarity_orthogonal_vectors_is_zero(broker):
    service = QueryService(broker)

    score = service.cosine_similarity([1.0, 0.0], [0.0, 1.0])

    assert score == 0.0


def test_cosine_similarity_zero_vector_returns_zero(broker):
    service = QueryService(broker)

    score = service.cosine_similarity([0.0, 0.0], [1.0, 1.0])

    assert score == 0.0


def test_query_service_publishes_query_completed(broker):
    service = QueryService(broker)
    service.start()

    received = []
    broker.subscribe(QUERY_COMPLETED, lambda event: received.append(event))

    query_embedding = service.create_embedding("car")
    insert_vector(broker, "img_1", ["car"], query_embedding)

    query_event = make_event(
        topic=QUERY_SUBMITTED,
        producer="test",
        event_id="evt_qry_1_submitted",
        payload={
            "query_id": "qry_1",
            "query_type": "text",
            "query_text": "car",
            "top_k": 1,
        },
    ).to_dict()

    broker.publish(QUERY_SUBMITTED, query_event)

    assert wait_until(lambda: len(received) == 1)

    output = received[0]
    assert output["topic"] == QUERY_COMPLETED
    assert output["payload"]["query_id"] == "qry_1"
    assert output["payload"]["results"][0]["image_id"] == "img_1"


def test_query_service_respects_top_k(broker):
    service = QueryService(broker)
    service.start()

    received = []
    broker.subscribe(QUERY_COMPLETED, lambda event: received.append(event))

    insert_vector(broker, "img_1", ["car"], service.create_embedding("car"))
    insert_vector(broker, "img_2", ["person"], service.create_embedding("person"))
    insert_vector(broker, "img_3", ["bike"], service.create_embedding("bike"))

    query_event = make_event(
        topic=QUERY_SUBMITTED,
        producer="test",
        event_id="evt_qry_topk",
        payload={
            "query_id": "qry_topk",
            "query_type": "text",
            "query_text": "car",
            "top_k": 2,
        },
    ).to_dict()

    broker.publish(QUERY_SUBMITTED, query_event)

    assert wait_until(lambda: len(received) == 1)

    results = received[0]["payload"]["results"]

    assert len(results) == 2


def test_query_service_returns_empty_results_when_no_vectors_exist(broker):
    service = QueryService(broker)
    service.start()

    received = []
    broker.subscribe(QUERY_COMPLETED, lambda event: received.append(event))

    query_event = make_event(
        topic=QUERY_SUBMITTED,
        producer="test",
        event_id="evt_qry_empty",
        payload={
            "query_id": "qry_empty",
            "query_type": "text",
            "query_text": "car",
            "top_k": 3,
        },
    ).to_dict()

    broker.publish(QUERY_SUBMITTED, query_event)

    assert wait_until(lambda: len(received) == 1)

    assert received[0]["payload"]["results"] == []