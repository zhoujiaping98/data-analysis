from __future__ import annotations

import json
from typing import Any, Dict, List

from backend.app.core.config import settings
from backend.app.core.llm import get_chat_client

ANALYSIS_PROMPT = """    You are a data analyst. You will receive:
- The user's question
- The SQL used
- A small sample of the query result rows (possibly truncated)
Provide:
1) Key findings (bullet points)
2) Caveats / data quality notes
3) Next queries to dig deeper (2-3 suggestions)
Keep it concise and actionable.
"""

async def analyze(question: str, sql: str, columns: List[str], rows: List[List[Any]]) -> str:
    client = get_chat_client()
    sample = rows[:50]
    payload = {
        "question": question,
        "sql": sql,
        "columns": columns,
        "sample_rows": sample,
        "row_count_shown": len(rows),
    }
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": ANALYSIS_PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    content = await client.chat(messages, temperature=0.2)
    return content.strip()
