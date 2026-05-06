import json
import os
from typing import Any

import psycopg


class PostgresDocumentStore:
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or os.getenv(
            "POSTGRES_DSN",
            "postgresql://postgres:postgres@localhost:5432/image_retrieval",
        )
        self._init_db()

    def _connect(self):
        return psycopg.connect(self.dsn)

    def _init_db(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS image_documents (
                        image_id TEXT PRIMARY KEY,
                        image_path TEXT NOT NULL,
                        annotated_path TEXT,
                        status TEXT NOT NULL,
                        object_count INTEGER NOT NULL DEFAULT 0,
                        document_json JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS object_annotations (
                        id BIGSERIAL PRIMARY KEY,
                        image_id TEXT NOT NULL REFERENCES image_documents(image_id) ON DELETE CASCADE,
                        object_id TEXT NOT NULL,
                        label TEXT,
                        confidence DOUBLE PRECISION,
                        bbox_json JSONB,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_image_documents_status
                    ON image_documents(status)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_image_documents_document_json
                    ON image_documents
                    USING GIN (document_json)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_object_annotations_image_id
                    ON object_annotations(image_id)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_object_annotations_label
                    ON object_annotations(label)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_object_annotations_bbox_json
                    ON object_annotations
                    USING GIN (bbox_json)
                """)

            conn.commit()

    def upsert_image_document(self, document: dict[str, Any]) -> None:
        image_id = document["image_id"]
        image_path = document["image_path"]
        annotated_path = document.get("annotated_path")
        status = document["status"]
        objects = document.get("objects", [])
        object_count = len(objects)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO image_documents (
                        image_id,
                        image_path,
                        annotated_path,
                        status,
                        object_count,
                        document_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %(image_id)s,
                        %(image_path)s,
                        %(annotated_path)s,
                        %(status)s,
                        %(object_count)s,
                        %(document_json)s::jsonb,
                        NOW(),
                        NOW()
                    )
                    ON CONFLICT (image_id)
                    DO UPDATE SET
                        image_path = EXCLUDED.image_path,
                        annotated_path = EXCLUDED.annotated_path,
                        status = EXCLUDED.status,
                        object_count = EXCLUDED.object_count,
                        document_json = EXCLUDED.document_json,
                        updated_at = NOW()
                """, {
                    "image_id": image_id,
                    "image_path": image_path,
                    "annotated_path": annotated_path,
                    "status": status,
                    "object_count": object_count,
                    "document_json": json.dumps(document),
                })

                cur.execute(
                    "DELETE FROM object_annotations WHERE image_id = %s",
                    (image_id,),
                )

                for obj in objects:
                    cur.execute("""
                        INSERT INTO object_annotations (
                            image_id,
                            object_id,
                            label,
                            confidence,
                            bbox_json
                        )
                        VALUES (%s, %s, %s, %s, %s::jsonb)
                    """, (
                        image_id,
                        obj.get("object_id"),
                        obj.get("label"),
                        obj.get("confidence"),
                        json.dumps(obj.get("bbox")),
                    ))

            conn.commit()

    def get_image_document(self, image_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT document_json
                    FROM image_documents
                    WHERE image_id = %s
                """, (image_id,))
                row = cur.fetchone()

                if row is None:
                    return None

                value = row[0]
                if isinstance(value, str):
                    return json.loads(value)
                return value

    def list_documents(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT document_json
                    FROM image_documents
                    ORDER BY updated_at DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()

                result = []
                for row in rows:
                    value = row[0]
                    if isinstance(value, str):
                        result.append(json.loads(value))
                    else:
                        result.append(value)
                return result

    def stats(self) -> dict[str, int]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM image_documents")
                docs_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM object_annotations")
                objects_count = cur.fetchone()[0]

        return {
            "documents": docs_count,
            "objects": objects_count,
        }