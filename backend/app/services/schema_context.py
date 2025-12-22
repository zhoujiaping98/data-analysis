from __future__ import annotations

from typing import List, Iterable

from backend.app.core.training import get_store

def build_schema_context(
    question: str,
    datasource_id: str,
    k: int = 6,
    allowed_tables: Iterable[str] | None = None,
) -> str:
    store = get_store(datasource_id)
    hits = store.search(question, k=max(k, 10))
    allowed = {t for t in (allowed_tables or []) if t}
    # Keep it compact to reduce tokens
    parts: List[str] = []
    for h in hits:
        if allowed:
            meta = h.get("metadata") or {}
            table = meta.get("table")
            if table not in allowed:
                continue
        parts.append(h["text"])
        if len(parts) >= k:
            break
    return "\n\n".join(parts)
