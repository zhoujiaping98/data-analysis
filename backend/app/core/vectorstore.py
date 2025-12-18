from __future__ import annotations

import logging
import os
from typing import List, Dict, Any

import chromadb
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

from backend.app.core.config import settings
from backend.app.core.llm import OpenAICompatEmbeddingClient, get_embed_client

log = logging.getLogger("vectorstore")


class RemoteEmbeddingFunction(EmbeddingFunction):
    def __init__(self, embed_client: OpenAICompatEmbeddingClient):
        self._client = embed_client

    def __call__(self, input: Documents) -> Embeddings:
        # Chroma expects sync embedding function. We bridge with a simple event loop run.
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            # If we're in an event loop, run in a new loop via asyncio.run in a thread.
            # Keep it simple; for heavy usage, replace with a proper async embedding pipeline.
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=1) as ex:
                return ex.submit(lambda: asyncio.run(self._client.embed(list(input)))).result()
        except RuntimeError:
            return asyncio.run(self._client.embed(list(input)))


class SchemaVectorStore:
    def __init__(self):
        os.makedirs(settings.CHROMA_PERSIST_DIR, exist_ok=True)

        # If embed config is missing, fall back to Chroma default embedding.
        embed_fn = None
        if settings.has_embed_config:
            try:
                embed_fn = RemoteEmbeddingFunction(get_embed_client())
            except Exception as e:
                log.warning("Embedding not ready, fallback to default. err=%s", e)

        self._client = chromadb.PersistentClient(path=settings.CHROMA_PERSIST_DIR)
        self._collection = self._client.get_or_create_collection(
            name=settings.CHROMA_COLLECTION,
            embedding_function=embed_fn,
            metadata={"hnsw:space": "cosine"},
        )

    def reset(self) -> None:
        try:
            self._client.delete_collection(settings.CHROMA_COLLECTION)
        except Exception:
            pass
        self._collection = self._client.get_or_create_collection(
            name=settings.CHROMA_COLLECTION
        )

    def upsert_schema_docs(self, docs: List[Dict[str, Any]]) -> None:
        # docs: [{id, text, metadata}]
        ids = [d["id"] for d in docs]
        texts = [d["text"] for d in docs]
        metas = [d.get("metadata", {}) for d in docs]
        self._collection.upsert(ids=ids, documents=texts, metadatas=metas)

    def search(self, query: str, k: int = 8) -> List[Dict[str, Any]]:
        res = self._collection.query(query_texts=[query], n_results=k)
        out: List[Dict[str, Any]] = []
        for i in range(len(res["ids"][0])):
            out.append(
                {
                    "id": res["ids"][0][i],
                    "text": res["documents"][0][i],
                    "metadata": res["metadatas"][0][i],
                    "distance": res["distances"][0][i],
                }
            )
        return out
