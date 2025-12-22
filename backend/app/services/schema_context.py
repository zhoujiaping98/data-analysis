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
    hits = store.search(question, k=max(k, 12))
    allowed = {t for t in (allowed_tables or []) if t}
    # Keep it compact to reduce tokens
    schema_parts: List[str] = []
    qa_parts: List[str] = []
    for h in hits:
        meta = h.get("metadata") or {}
        doc_type = meta.get("type") or "schema"
        if doc_type == "qa":
            if allowed:
                tables = meta.get("tables") or []
                if not isinstance(tables, list):
                    tables = []
                if not any(t in allowed for t in tables):
                    continue
            qa_parts.append(h["text"])
            if len(qa_parts) >= max(2, k // 2):
                continue
        else:
            if allowed:
                table = meta.get("table")
                if table not in allowed:
                    continue
            schema_parts.append(h["text"])
            if len(schema_parts) >= k:
                continue
        if len(schema_parts) >= k and len(qa_parts) >= max(2, k // 2):
            break

    out: List[str] = []
    if schema_parts:
        out.append("\n\n".join(schema_parts))
    if qa_parts:
        out.append("Example Q&A:\n" + "\n\n".join(qa_parts))
    return "\n\n".join(out)
