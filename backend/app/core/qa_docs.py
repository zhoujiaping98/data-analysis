from __future__ import annotations

from typing import Dict, Any, List


def build_qa_doc(
    qa_id: str,
    ds_id: str,
    question: str,
    sql: str,
    note: str | None,
    tables: List[str],
    tags: List[str],
) -> Dict[str, Any]:
    note_text = f"\nNote: {note}" if note else ""
    tables_text = ", ".join(tables) if tables else ""
    tags_text = ", ".join(tags) if tags else ""
    text = f"Q: {question}\nSQL: {sql}"
    if tables_text:
        text += f"\nTables: {tables_text}"
    if tags_text:
        text += f"\nTags: {tags_text}"
    text += note_text
    return {
        "id": f"qa::{qa_id}",
        "text": text,
        "metadata": {"type": "qa", "tables": tables, "tags": tags, "datasource_id": ds_id},
    }
