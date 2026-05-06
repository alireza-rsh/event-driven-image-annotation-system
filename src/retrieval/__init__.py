from __future__ import annotations

from dataclasses import dataclass
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
    item_type: str          # "full_image" or "object_crop"
    image_id: str
    image_path: str
    annotated_path: str | None
    object_id: str | None
    label: str | None
    confidence: float | None
    bbox_xyxy: list[float] | None


class ClipEmbedder:
    def __init__(self, model_name: str = "openai/clip-vit-base-patch32", device: str | None = None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.processor = AutoProcessor.from_pretrained(model_name)
        self.model = CLIPModel.from_pretrained(model_name).to(self.device)
        self.model.eval()

    @torch.inference_mode()
    def encode_image(self, image: Image.Image) -> np.ndarray:
        inputs = self.processor(images=image, return_tensors="pt").to(self.device)
        feats = self.model.get_image_features(**inputs)
        vec = feats.detach().cpu().numpy().astype("float32")
        faiss.normalize_L2(vec)
        return vec[0]

    @torch.inference_mode()
    def encode_text(self, text: str) -> np.ndarray:
        inputs = self.processor(text=[text], return_tensors="pt", padding=True).to(self.device)
        feats = self.model.get_text_features(**inputs)
        vec = feats.detach().cpu().numpy().astype("float32")
        faiss.normalize_L2(vec)
        return vec[0]


class FaissStore:
    def __init__(self, dim: int):
        base = faiss.IndexFlatIP(dim)
        self.index = faiss.IndexIDMap2(base)
        self.meta: dict[int, IndexedItem] = {}

    def add(self, item_id: int, vector: np.ndarray, metadata: IndexedItem) -> None:
        vec = vector.reshape(1, -1).astype("float32")
        faiss.normalize_L2(vec)
        self.index.add_with_ids(vec, np.array([item_id], dtype=np.int64))
        self.meta[item_id] = metadata

    def search(self, vector: np.ndarray, k: int = 5) -> list[dict[str, Any]]:
        if self.index.ntotal == 0:
            return []

        q = vector.reshape(1, -1).astype("float32")
        faiss.normalize_L2(q)
        scores, ids = self.index.search(q, k)

        results = []
        for score, idx in zip(scores[0], ids[0]):
            if idx == -1:
                continue
            meta = self.meta[int(idx)]
            results.append({
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
            })
        return results


class RealImageRetrievalSystem:
    def __init__(self, detector_model: str = "yolo26n.pt"):
        self.detector = YOLO(detector_model)
        self.embedder = ClipEmbedder()

        # initialize FAISS dimension from one dummy text embedding
        dim = self.embedder.encode_text("test").shape[0]
        self.store = FaissStore(dim=dim)

        self.next_id = 1

    def _new_faiss_id(self) -> int:
        current = self.next_id
        self.next_id += 1
        return current

    def _crop_xyxy(self, image: Image.Image, xyxy: list[float]) -> Image.Image:
        x1, y1, x2, y2 = map(int, xyxy)
        return image.crop((x1, y1, x2, y2))

    def index_image(self, image_path: str, output_dir: str = "artifacts") -> dict[str, Any]:
        image_path = str(Path(image_path).resolve())
        image = Image.open(image_path).convert("RGB")
        image_id = f"img_{uuid.uuid4().hex[:12]}"

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1) detect objects
        results = self.detector(image_path)
        result = results[0]

        # 2) save annotated image with boxes
        annotated_path = str(output_dir / f"{image_id}_annotated.jpg")
        result.save(filename=annotated_path)

        # 3) index full-image embedding
        full_vec = self.embedder.encode_image(image)
        full_id = self._new_faiss_id()
        self.store.add(
            full_id,
            full_vec,
            IndexedItem(
                faiss_id=full_id,
                item_type="full_image",
                image_id=image_id,
                image_path=image_path,
                annotated_path=annotated_path,
                object_id=None,
                label=None,
                confidence=None,
                bbox_xyxy=None,
            ),
        )

        detections = []

        # 4) index each detected object crop
        boxes = result.boxes
        if boxes is not None:
            xyxy = boxes.xyxy.cpu().numpy()
            confs = boxes.conf.cpu().numpy()
            clses = boxes.cls.cpu().numpy().astype(int)

            for i, (box, conf, cls_id) in enumerate(zip(xyxy, confs, clses), start=1):
                label = result.names[int(cls_id)]
                crop = self._crop_xyxy(image, box.tolist())
                crop_vec = self.embedder.encode_image(crop)

                faiss_id = self._new_faiss_id()
                object_id = f"obj_{i:04d}"

                self.store.add(
                    faiss_id,
                    crop_vec,
                    IndexedItem(
                        faiss_id=faiss_id,
                        item_type="object_crop",
                        image_id=image_id,
                        image_path=image_path,
                        annotated_path=annotated_path,
                        object_id=object_id,
                        label=label,
                        confidence=float(conf),
                        bbox_xyxy=[float(v) for v in box.tolist()],
                    ),
                )

                detections.append({
                    "object_id": object_id,
                    "label": label,
                    "confidence": float(conf),
                    "bbox_xyxy": [float(v) for v in box.tolist()],
                })

        return {
            "image_id": image_id,
            "image_path": image_path,
            "annotated_path": annotated_path,
            "detections": detections,
        }

    def search_by_text(self, text: str, k: int = 5) -> list[dict[str, Any]]:
        q = self.embedder.encode_text(text)
        return self.store.search(q, k=k)

    def search_by_image(self, image_path: str, k: int = 5) -> list[dict[str, Any]]:
        image = Image.open(image_path).convert("RGB")
        q = self.embedder.encode_image(image)
        return self.store.search(q, k=k)

    def save_metadata(self, path: str = "artifacts/index_metadata.json") -> None:
        items = []
        for _, meta in sorted(self.store.meta.items(), key=lambda x: x[0]):
            items.append({
                "faiss_id": meta.faiss_id,
                "item_type": meta.item_type,
                "image_id": meta.image_id,
                "image_path": meta.image_path,
                "annotated_path": meta.annotated_path,
                "object_id": meta.object_id,
                "label": meta.label,
                "confidence": meta.confidence,
                "bbox_xyxy": meta.bbox_xyxy,
            })

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)