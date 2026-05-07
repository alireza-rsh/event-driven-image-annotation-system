import pytest


def test_mongo_document_store_upsert_get_list_stats(monkeypatch):
    mongomock = pytest.importorskip("mongomock")

    import storage.mongo_document_store as mongo_module

    monkeypatch.setattr(mongo_module, "MongoClient", mongomock.MongoClient)

    store = mongo_module.MongoDocumentStore(
        uri="mongodb://localhost:27017/",
        db_name="test_db",
        collection_name="test_docs",
    )

    store.upsert_image_document(
        {
            "image_id": "img_1",
            "image_path": "images/a.jpg",
            "annotated_path": "artifacts/a.jpg",
            "status": "success",
            "objects": [
                {"object_id": "obj_1", "label": "horse", "bbox": [1, 2, 3, 4], "confidence": 0.9}
            ],
            "object_count": 1,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
            "history": ["image.submitted", "inference.completed"],
        }
    )

    doc = store.get_image_document("img_1")
    assert doc is not None
    assert doc["image_id"] == "img_1"
    assert doc["objects"][0]["label"] == "horse"

    docs = store.list_documents(limit=10)
    assert len(docs) == 1

    stats = store.stats()
    assert stats["documents"] == 1
    assert stats["objects"] == 1
