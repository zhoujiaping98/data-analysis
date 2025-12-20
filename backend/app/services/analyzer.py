from __future__ import annotations

import json
from typing import Any, AsyncGenerator, Dict, List

from backend.app.core.config import settings
from backend.app.core.llm import get_chat_client

ANALYSIS_PROMPT = """你是一名数据分析师。你会收到：
- 用户问题
- 实际执行的 SQL
- 查询结果的部分样本行（可能被截断）

请用中文输出，并包含：
1）关键结论（要点列表）
2）口径说明 / 数据质量与局限（要点列表）
3）下一步建议（2-3 条可执行的后续查询方向）

要求：简洁、可落地，不要输出 Markdown 代码块。
"""

def _build_messages(question: str, sql: str, columns: List[str], rows: List[List[Any]]) -> List[Dict[str, str]]:
    sample = rows[:50]
    payload = {
        "question": question,
        "sql": sql,
        "columns": columns,
        "sample_rows": sample,
        "row_count_shown": len(rows),
    }
    return [
        {"role": "system", "content": ANALYSIS_PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]


async def analyze_stream(
    question: str, sql: str, columns: List[str], rows: List[List[Any]]
) -> AsyncGenerator[str, None]:
    client = get_chat_client()
    messages = _build_messages(question, sql, columns, rows)
    try:
        async for chunk in client.chat_stream(messages, temperature=0.2):
            yield chunk
    except Exception:
        try:
            content = await client.chat(messages, temperature=0.2)
            if content:
                yield content.strip()
        except Exception:
            yield "分析服务暂时不可用，请稍后再试。"


async def analyze(question: str, sql: str, columns: List[str], rows: List[List[Any]]) -> str:
    parts: List[str] = []
    async for chunk in analyze_stream(question, sql, columns, rows):
        parts.append(chunk)
    return "".join(parts).strip()
