from __future__ import annotations

from typing import List

from backend.app.core.training import get_store

def build_schema_context(question: str, k: int = 6) -> str:
    store = get_store()
    hits = store.search(question, k=k)
    # Keep it compact to reduce tokens
    parts: List[str] = []
    for h in hits:
        parts.append(h["text"])
    return "\n\n".join(parts)
