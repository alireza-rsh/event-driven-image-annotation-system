import time
from typing import Any


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.1) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class InMemoryDocumentStore:
    def __init__(self):
        self.documents: dict[str, dict[str, Any]] = {}

    def upsert_image_document(self, document: dict[str, Any]) -> None:
        self.documents[document["image_id"]] = document

    def get_image_document(self, image_id: str) -> dict[str, Any] | None:
        return self.documents.get(image_id)

    def list_documents(self, limit: int = 20) -> list[dict[str, Any]]:
        docs = list(self.documents.values())
        docs.sort(key=lambda d: d.get("updated_at", ""), reverse=True)
        return docs[:limit]

    def stats(self) -> dict[str, int]:
        documents = len(self.documents)
        objects = sum(len(doc.get("objects", [])) for doc in self.documents.values())
        return {
            "documents": documents,
            "objects": objects,
        }