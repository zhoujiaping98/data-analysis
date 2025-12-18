from __future__ import annotations

from typing import List, Dict

from backend.app.core.config import settings
from backend.app.core.llm import get_chat_client

SYSTEM_PROMPT = """    You are an expert data analyst. Convert the user's question into a SINGLE MySQL query.
Rules:
- Only output SQL (no markdown, no explanations).
- Only SELECT/WITH queries. Never use INSERT/UPDATE/DELETE/DDL.
- Prefer explicit column names; avoid SELECT * unless necessary.
- Use LIMIT {max_rows} unless the user explicitly asks for all rows.
- If the question is ambiguous, make a reasonable assumption and still output SQL.
"""

def build_messages(question: str, schema_context: str, history: List[Dict[str, str]]) -> List[Dict[str, str]]:
    msgs: List[Dict[str, str]] = []
    msgs.append({"role": "system", "content": SYSTEM_PROMPT.format(max_rows=settings.MAX_ROWS)})
    if schema_context:
        msgs.append({"role": "system", "content": f"Relevant schema:\n{schema_context}"})
    # history: [{'role': 'user'|'assistant', 'content': '...'}]
    msgs.extend(history[-10:])
    msgs.append({"role": "user", "content": question})
    return msgs


async def generate_sql(question: str, schema_context: str, history: List[Dict[str, str]]) -> str:
    client = get_chat_client()
    content = await client.chat(build_messages(question, schema_context, history), temperature=settings.LLM_TEMPERATURE)
    return content.strip().strip("` ")
