from __future__ import annotations

import logging

from backend.app.core.config import settings
from backend.app.core.vectorstore import SchemaVectorStore
from backend.app.core.mysql import fetch_schema_documents

log = logging.getLogger("training")

_store: SchemaVectorStore | None = None

def get_store() -> SchemaVectorStore:
    global _store
    if _store is None:
        _store = SchemaVectorStore()
    return _store


async def train_schema_on_startup() -> None:
    if not settings.has_mysql_config:
        log.warning("MySQL config is empty; skip schema training at startup.")
        return

    store = get_store()
    try:
        docs = await fetch_schema_documents()
        if not docs:
            log.warning("No schema docs fetched; check MySQL permissions.")
            return
        store.upsert_schema_docs(docs)
        log.info("Schema training finished. docs=%d", len(docs))
    except Exception as e:
        log.exception("Schema training failed: %s", e)
