from __future__ import annotations

import logging
import os
import re
import math
import hashlib
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


class LocalHashEmbeddingFunction(EmbeddingFunction):
    """Lightweight, offline embedding fallback.

    This avoids Chroma's default embedding, which may download large ONNX models at runtime.
    It is not semantically strong, but is good enough for keyword-ish schema retrieval.
    """

    def __init__(self, dim: int = 384):
        self._dim = dim
        self._token_re = re.compile(r"[A-Za-z0-9_]+")

    def _embed_one(self, text: str) -> List[float]:
        vec = [0.0] * self._dim
        tokens = self._token_re.findall((text or "").lower())
        if not tokens:
            return vec

        for tok in tokens:
            h = hashlib.blake2b(tok.encode("utf-8"), digest_size=8).digest()
            idx = int.from_bytes(h, "little") % self._dim
            vec[idx] += 1.0

        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            inv = 1.0 / norm
            vec = [v * inv for v in vec]
        return vec

    def __call__(self, input: Documents) -> Embeddings:
        return [self._embed_one(t) for t in list(input)]


class ResilientEmbeddingFunction(EmbeddingFunction):
    def __init__(self, embed_client: OpenAICompatEmbeddingClient, fallback_dim: int = 384):
        self._remote = RemoteEmbeddingFunction(embed_client)
        self._dim: int | None = None
        self._fallback_dim = fallback_dim
        self._fallback = LocalHashEmbeddingFunction(dim=fallback_dim)

    def _ensure_dim(self, dim: int) -> None:
        if self._dim is None:
            self._dim = dim
            if self._fallback_dim != dim:
                self._fallback_dim = dim
                self._fallback = LocalHashEmbeddingFunction(dim=dim)
            return
        if dim != self._dim:
            raise ValueError(f"Embedding dimension changed: {self._dim} -> {dim}")

    def __call__(self, input: Documents) -> Embeddings:
        try:
            out = self._remote(input)
            if out:
                self._ensure_dim(len(out[0]))
            return out
        except Exception as e:
            log.warning("Remote embedding failed; using local fallback. err=%s", e)
            if self._dim is not None and self._fallback_dim != self._dim:
                self._fallback_dim = self._dim
                self._fallback = LocalHashEmbeddingFunction(dim=self._dim)
            return self._fallback(input)


class SchemaVectorStore:
    def __init__(self):
        os.makedirs(settings.CHROMA_PERSIST_DIR, exist_ok=True)

        # Prefer remote embeddings when configured; otherwise use a lightweight offline fallback.
        embed_fn: EmbeddingFunction = LocalHashEmbeddingFunction()
        if settings.has_embed_config:
            try:
                embed_fn = ResilientEmbeddingFunction(get_embed_client())
            except Exception as e:
                log.warning("Embedding not ready, fallback to local hashing. err=%s", e)

        self._client = chromadb.PersistentClient(path=settings.CHROMA_PERSIST_DIR)
        self._embed_fn = embed_fn
        self._collection_metadata = {"hnsw:space": "cosine"}
        self._collection = self._get_or_create_collection()

    def _get_or_create_collection(self):
        return self._client.get_or_create_collection(
            name=settings.CHROMA_COLLECTION,
            embedding_function=self._embed_fn,
            metadata=self._collection_metadata,
        )

    def reset(self) -> None:
        try:
            self._client.delete_collection(settings.CHROMA_COLLECTION)
        except Exception:
            pass
        self._collection = self._get_or_create_collection()

    def upsert_schema_docs(self, docs: List[Dict[str, Any]]) -> None:
        # docs: [{id, text, metadata}]
        ids = [d["id"] for d in docs]
        texts = [d["text"] for d in docs]
        metas = [d.get("metadata", {}) for d in docs]
        try:
            self._collection.upsert(ids=ids, documents=texts, metadatas=metas)
            return
        except Exception as e:
            msg = str(e)
            if "expecting embedding with dimension" in msg.lower():
                log.warning("Embedding dimension changed; resetting collection and retrying. err=%s", e)
                self.reset()
                self._collection.upsert(ids=ids, documents=texts, metadatas=metas)
                return
            raise

    def search(self, query: str, k: int = 8) -> List[Dict[str, Any]]:
        try:
            res = self._collection.query(query_texts=[query], n_results=k)
        except Exception as e:
            log.warning("Vector search failed; returning empty hits. err=%s", e)
            return []
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
