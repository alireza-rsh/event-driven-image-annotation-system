import os
from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection


class MongoDocumentStore:
    def __init__(
        self,
        uri: str | None = None,
        db_name: str | None = None,
        collection_name: str | None = None,
    ):
        self.uri = uri or os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.db_name = db_name or os.getenv("MONGO_DB", "image_retrieval")
        self.collection_name = collection_name or os.getenv("MONGO_COLLECTION", "image_documents")

        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)

        # Fail fast if MongoDB is unreachable
        self.client.admin.command("ping")

        self.db = self.client[self.db_name]
        self.collection: Collection = self.db[self.collection_name]

        self._init_indexes()

    def _init_indexes(self) -> None:
        self.collection.create_index("image_id", unique=True)
        self.collection.create_index("status")
        self.collection.create_index("objects.label")

    @staticmethod
    def _normalize_outgoing_document(doc: dict[str, Any] | None) -> dict[str, Any] | None:
        if doc is None:
            return None

        doc = dict(doc)
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def upsert_image_document(self, document: dict[str, Any]) -> None:
        image_id = document["image_id"]

        self.collection.update_one(
            {"image_id": image_id},
            {"$set": document},
            upsert=True,
        )

    def get_image_document(self, image_id: str) -> dict[str, Any] | None:
        doc = self.collection.find_one({"image_id": image_id})
        return self._normalize_outgoing_document(doc)

    def list_documents(self, limit: int = 20) -> list[dict[str, Any]]:
        docs = list(
            self.collection.find().sort("updated_at", -1).limit(limit)
        )
        return [self._normalize_outgoing_document(doc) for doc in docs]

    def stats(self) -> dict[str, int]:
        documents = self.collection.count_documents({})

        pipeline = [
            {
                "$project": {
                    "object_count": {
                        "$size": {"$ifNull": ["$objects", []]}
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_objects": {"$sum": "$object_count"}
                }
            }
        ]

        agg = list(self.collection.aggregate(pipeline))
        objects = agg[0]["total_objects"] if agg else 0

        return {
            "documents": documents,
            "objects": objects,
        }