from __future__ import annotations

import logging

from backend.app.core.config import settings
from backend.app.core.vectorstore import SchemaVectorStore
from backend.app.core.mysql import fetch_schema_documents

log = logging.getLogger("training")

_stores: dict[str, SchemaVectorStore] = {}

def get_store(datasource_id: str) -> SchemaVectorStore:
    if datasource_id not in _stores:
        _stores[datasource_id] = SchemaVectorStore(collection_suffix=datasource_id)
    return _stores[datasource_id]


async def train_schema_on_startup() -> None:
    if not settings.has_mysql_config:
        log.warning("MySQL config is empty; skip schema training at startup.")
        return

    store = get_store("default")
    try:
        docs = await fetch_schema_documents()
        if not docs:
            log.warning("No schema docs fetched; check MySQL permissions.")
            return
        store.upsert_schema_docs(docs)
        log.info("Schema training finished. docs=%d", len(docs))
    except Exception as e:
        log.exception("Schema training failed: %s", e)
