from __future__ import annotations

from typing import List, Dict
import re

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

_CODE_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
_SQL_PREFIX_RE = re.compile(r"^\s*sql\s*:\s*", re.IGNORECASE)
_START_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_FIRST_START_RE = re.compile(r"\b(SELECT|WITH)\b", re.IGNORECASE)


def _extract_sql(text: str) -> str:
    s = (text or "").strip()

    # Unwrap ```sql ... ``` fences if present.
    m = _CODE_FENCE_RE.search(s)
    if m:
        s = m.group(1).strip()

    # Remove "SQL:" prefix if the model includes it.
    s = _SQL_PREFIX_RE.sub("", s).strip()

    # If the output includes prose before the SQL, cut to the first SELECT/WITH.
    if not _START_RE.search(s):
        m2 = _FIRST_START_RE.search(s)
        if m2:
            s = s[m2.start() :].strip()

    # If multiple statements were returned, keep only the first.
    semi = s.find(";")
    if semi != -1 and s[semi + 1 :].strip():
        s = s[:semi].strip()

    return s.strip().strip("` ")


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
    return _extract_sql(content)
