from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
import json
import uuid

import faiss
import numpy as np
import torch
from PIL import Image
from transformers import AutoProcessor, CLIPModel
from ultralytics import YOLO


@dataclass
class IndexedItem:
    faiss_id: int
    item_type: str               # "full_image" or "object_crop"
    image_id: str
    image_path: str
    annotated_path: str | None
    object_id: str | None
    label: str | None
    confidence: float | None
    bbox_xyxy: list[float] | None


class ClipEmbedder:
    def __init__(
        self,
        model_name: str = "openai/clip-vit-base-patch32",
        device: str | None = None,
    ) -> None:
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.processor = AutoProcessor.from_pretrained(model_name)
        self.model = CLIPModel.from_pretrained(model_name).to(self.device)
        self.model.eval()

    @torch.inference_mode()
    def encode_image(self, image: Image.Image) -> np.ndarray:
        inputs = self.processor(images=image, return_tensors="pt").to(self.device)

        outputs = self.model.vision_model(pixel_values=inputs["pixel_values"])
        pooled = outputs.pooler_output
        features = self.model.visual_projection(pooled)

        vector = features.detach().cpu().numpy().astype("float32")
        faiss.normalize_L2(vector)
        return vector[0]

    @torch.inference_mode()
    def encode_text(self, text: str) -> np.ndarray:
        inputs = self.processor(text=[text], return_tensors="pt", padding=True).to(self.device)

        outputs = self.model.text_model(
            input_ids=inputs["input_ids"],
            attention_mask=inputs.get("attention_mask"),
        )
        pooled = outputs.pooler_output
        features = self.model.text_projection(pooled)

        vector = features.detach().cpu().numpy().astype("float32")
        faiss.normalize_L2(vector)
        return vector[0]


class FaissStore:
    def __init__(self, dim: int) -> None:
        base_index = faiss.IndexFlatIP(dim)
        self.index = faiss.IndexIDMap2(base_index)
        self.meta: dict[int, IndexedItem] = {}

    def add(self, item_id: int, vector: np.ndarray, metadata: IndexedItem) -> None:
        vec = vector.reshape(1, -1).astype("float32")
        faiss.normalize_L2(vec)
        ids = np.array([item_id], dtype=np.int64)
        self.index.add_with_ids(vec, ids)
        self.meta[item_id] = metadata

    def search(self, vector: np.ndarray, k: int = 5) -> list[dict[str, Any]]:
        if self.index.ntotal == 0:
            return []

        query = vector.reshape(1, -1).astype("float32")
        faiss.normalize_L2(query)

        scores, ids = self.index.search(query, k)
        results: list[dict[str, Any]] = []

        for score, idx in zip(scores[0], ids[0]):
            if idx == -1:
                continue

            meta = self.meta.get(int(idx))
            if meta is None:
                continue

            results.append(
                {
                    "faiss_id": int(idx),
                    "score": float(score),
                    "item_type": meta.item_type,
                    "image_id": meta.image_id,
                    "image_path": meta.image_path,
                    "annotated_path": meta.annotated_path,
                    "object_id": meta.object_id,
                    "label": meta.label,
                    "confidence": meta.confidence,
                    "bbox_xyxy": meta.bbox_xyxy,
                }
            )

        return results

    def save(self, index_path: str, metadata_path: str) -> None:
        Path(index_path).parent.mkdir(parents=True, exist_ok=True)
        Path(metadata_path).parent.mkdir(parents=True, exist_ok=True)

        faiss.write_index(self.index, str(index_path))

        payload = {
            "items": [asdict(item) for _, item in sorted(self.meta.items(), key=lambda x: x[0])]
        }
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def load(self, index_path: str, metadata_path: str) -> None:
        self.index = faiss.read_index(str(index_path))

        with open(metadata_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        self.meta = {}
        for raw in payload.get("items", []):
            item = IndexedItem(**raw)
            self.meta[item.faiss_id] = item

    def derive_next_id(self) -> int:
        if not self.meta:
            return 1
        return max(self.meta.keys()) + 1


class RealImageRetrievalSystem:
    def __init__(
        self,
        detector_model: str = "yolo11n.pt",
        clip_model: str = "openai/clip-vit-base-patch32",
        device: str | None = None,
        index_dir: str = "artifacts/faiss",
    ) -> None:
        self.detector = YOLO(detector_model)
        self.embedder = ClipEmbedder(model_name=clip_model, device=device)

        dim = self.embedder.encode_text("test").shape[0]
        self.store = FaissStore(dim=dim)

        self.index_dir = Path(index_dir)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.index_dir / "index.faiss"
        self.metadata_path = self.index_dir / "metadata.json"

        if self.index_path.exists() and self.metadata_path.exists():
            self.store.load(str(self.index_path), str(self.metadata_path))

        self.next_id = self.store.derive_next_id()

    def _new_faiss_id(self) -> int:
        current = self.next_id
        self.next_id += 1
        return current

    @staticmethod
    def _safe_crop(image: Image.Image, xyxy: list[float]) -> Image.Image:
        x1, y1, x2, y2 = xyxy
        width, height = image.size

        x1 = max(0, min(int(x1), width - 1))
        y1 = max(0, min(int(y1), height - 1))
        x2 = max(x1 + 1, min(int(x2), width))
        y2 = max(y1 + 1, min(int(y2), height))

        return image.crop((x1, y1, x2, y2))

    def _persist(self) -> None:
        self.store.save(str(self.index_path), str(self.metadata_path))

    def index_image(
        self,
        image_path: str,
        image_id: str | None = None,
        output_dir: str = "artifacts/annotated",
    ) -> dict[str, Any]:
        path = Path(image_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Image not found: {path}")

        final_image_id = image_id or f"img_{uuid.uuid4().hex[:12]}"
        image = Image.open(path).convert("RGB")

        # 1) Detect objects
        results = self.detector(str(path))
        result = results[0]

        # 2) Save annotated image
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        annotated_path = str(output_dir_path / f"{final_image_id}_annotated.jpg")
        result.save(filename=annotated_path)

        # 3) Index full image embedding
        full_vector = self.embedder.encode_image(image)
        full_id = self._new_faiss_id()

        self.store.add(
            item_id=full_id,
            vector=full_vector,
            metadata=IndexedItem(
                faiss_id=full_id,
                item_type="full_image",
                image_id=final_image_id,
                image_path=str(path),
                annotated_path=annotated_path,
                object_id=None,
                label=None,
                confidence=None,
                bbox_xyxy=None,
            ),
        )

        # 4) Index each detected object crop
        detections: list[dict[str, Any]] = []
        boxes = result.boxes

        if boxes is not None and len(boxes) > 0:
            xyxy = boxes.xyxy.cpu().numpy()
            confs = boxes.conf.cpu().numpy()
            class_ids = boxes.cls.cpu().numpy().astype(int)

            for i, (box, conf, cls_id) in enumerate(zip(xyxy, confs, class_ids), start=1):
                object_id = f"obj_{i:04d}"
                label = result.names[int(cls_id)]
                bbox_list = [float(v) for v in box.tolist()]

                crop = self._safe_crop(image, bbox_list)
                crop_vector = self.embedder.encode_image(crop)

                crop_faiss_id = self._new_faiss_id()
                self.store.add(
                    item_id=crop_faiss_id,
                    vector=crop_vector,
                    metadata=IndexedItem(
                        faiss_id=crop_faiss_id,
                        item_type="object_crop",
                        image_id=final_image_id,
                        image_path=str(path),
                        annotated_path=annotated_path,
                        object_id=object_id,
                        label=label,
                        confidence=float(conf),
                        bbox_xyxy=bbox_list,
                    ),
                )

                detections.append(
                    {
                        "object_id": object_id,
                        "label": label,
                        "confidence": float(conf),
                        "bbox": bbox_list,
                    }
                )

        self._persist()

        return {
            "image_id": final_image_id,
            "image_path": str(path),
            "annotated_path": annotated_path,
            "detections": detections,
            "indexed_items": 1 + len(detections),
            "faiss_total": int(self.store.index.ntotal),
        }

    def search_by_text(self, text: str, k: int = 5) -> list[dict[str, Any]]:
        if not text or not text.strip():
            raise ValueError("text query must not be empty")

        vector = self.embedder.encode_text(text.strip())
        return self.store.search(vector, k=k)

    def search_by_image(self, image_path: str, k: int = 5) -> list[dict[str, Any]]:
        path = Path(image_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Query image not found: {path}")

        image = Image.open(path).convert("RGB")
        vector = self.embedder.encode_image(image)
        return self.store.search(vector, k=k)

    def stats(self) -> dict[str, Any]:
        full_images = sum(1 for item in self.store.meta.values() if item.item_type == "full_image")
        object_crops = sum(1 for item in self.store.meta.values() if item.item_type == "object_crop")

        return {
            "faiss_total": int(self.store.index.ntotal),
            "full_images": full_images,
            "object_crops": object_crops,
            "index_path": str(self.index_path),
            "metadata_path": str(self.metadata_path),
        }

    def reset(self) -> None:
        dim = self.embedder.encode_text("test").shape[0]
        self.store = FaissStore(dim=dim)
        self.next_id = 1

        if self.index_path.exists():
            self.index_path.unlink()
        if self.metadata_path.exists():
            self.metadata_path.unlink()


if __name__ == "__main__":
    system = RealImageRetrievalSystem()

    print("Stats before indexing:", system.stats())

    # Example:
    # info = system.index_image("images/street.jpg", image_id="img_demo_001")
    # print(info)
    #
    # print(system.search_by_text("car", k=5))
    # print(system.search_by_image("images/query.jpg", k=5))