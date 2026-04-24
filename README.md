# Event-Driven Image Annotation & Retrieval System

This project implements an asynchronous, event-driven pipeline using Redis Pub/Sub.

---

## Overview

This system is designed using an **event-driven architecture** where services communicate through Redis topics instead of direct calls.

## Project Structure

```text
event-driven-image-annotation-system/
├── src/
│   ├── main.py
│   ├── event_generator.py
│   ├── messaging/
│   │   ├── __init__.py
│   │   ├── topics.py
│   │   ├── events.py
│   │   └── redis_broker.py
│   └── services/
│       ├── __init__.py
│       ├── cli_service.py
│       ├── inference_service.py
│       ├── document_db_service.py
│       ├── embedding_service.py
│       └── query_service.py
├── tests/
│   ├── conftest.py
│   ├── test_events.py
│   ├── test_redis_broker.py
│   ├── test_cli_service.py
│   ├── test_inference_service.py
│   ├── test_document_db_service.py
│   ├── test_embedding_service.py
│   ├── test_query_service.py
│   ├── test_integration_pipeline.py
│   └── test_failure_modes.py
├── images/
│   └── car_001.jpg
├── requirements.txt
├── pytest.ini
└── README.md
```

### Key Features

- Fully **asynchronous processing**
- Event-driven communication (Redis Pub/Sub)
- Modular services
- Loose coupling
- High testability

---

## Asynchronous Design

Each service:
- Subscribes to Redis topics
- Runs in a background thread
- Processes events independently

Flow is **non-blocking**:

User uploads image → event published → processing happens in background

---

## Architecture

### Image Processing Pipeline

CLI → image.submitted → Inference → inference.completed  
→ DocumentDB → annotation.stored → Embedding → embedding.created

### Query Pipeline

query.submitted → QueryService → query.completed → CLI

---

## Simulated Components (IMPORTANT)

This project intentionally **does NOT use real AI models**.

### Inference (Simulated)

Based on filename:

- images/car.jpg → label = car
- images/person.jpg → label = person

### Embeddings (Simulated)

Generated using hashing:

"car" → [0.12, 0.87, ...]

No deep learning is used.

---

## Setup

```bash
pip install -r requirements.txt
sudo systemctl start redis-server
redis-cli ping
```

---

##  Run

### Interactive mode

```bash
python src/main.py --interactive
```

---

## Tests

```bash
pytest -q
```

---
